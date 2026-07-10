"""Fleet simulation: weather + physics -> unified hourly generation frame.

Ties the pieces together with a physically-motivated merit order:

1. Wind and solar output are computed from the (real or synthetic) weather.
2. A synthetic system demand curve defines net load = demand - renewables.
3. Batteries dispatch against net load (charge on surplus, discharge on peak).
4. Gas units fill the residual after storage (CCGT mid-merit, peaker on top).
5. Renewable oversupply (renewables > demand) is booked as curtailment.

The result is one long-format frame with a shared core schema plus nullable,
technology-specific columns (battery SOC/throughput, gas fuel/heat-rate/CO2).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import polars as pl

from weather_analytics.mock_data import physics
from weather_analytics.mock_data.fleet import (
    BATTERY,
    FLEET,
    GAS,
    SOLAR,
    WIND,
    FleetAsset,
)
from weather_analytics.mock_data.weather_sources import get_weather

# Core + technology-specific columns emitted by the simulation.
GENERATION_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "asset_id",
    "asset_type",
    "gross_generation_mwh",
    "net_generation_mwh",
    "curtailment_mwh",
    "availability_pct",
    "asset_capacity_mw",
    "soc_pct",
    "charge_mwh",
    "discharge_mwh",
    "fuel_mmbtu",
    "heat_rate_btu_kwh",
    "co2_tonnes",
)


@dataclass(frozen=True)
class SimulationResult:
    """Bundle of the simulation outputs.

    Attributes
    ----------
    generation : pl.DataFrame
        Long-format hourly generation in :data:`GENERATION_COLUMNS`.
    weather : pl.DataFrame
        The weather frame that drove the simulation.
    weather_source : str
        ``"open-meteo"`` or ``"synthetic"``.
    """

    generation: pl.DataFrame
    weather: pl.DataFrame
    weather_source: str


def _rank_signal(values: np.ndarray) -> np.ndarray:
    """Map an array to its [0, 1] rank percentile (stable dispatch signal)."""
    n = values.shape[0]
    if n == 0:
        return values
    order = values.argsort()
    ranks = np.empty(n)
    ranks[order] = np.arange(n)
    return np.asarray(ranks / max(n - 1, 1))


def _demand_curve(timestamps: pl.Series, scale_mw: float) -> np.ndarray:
    """Synthetic system demand (MW): diurnal + seasonal shape, evening peak."""
    hours = timestamps.dt.hour().to_numpy()
    doy = timestamps.dt.ordinal_day().to_numpy()
    diurnal = 0.72 + 0.20 * np.sin(2 * np.pi * (hours - 17) / 24)
    # Summer + winter peaks (cooling/heating), spring/fall troughs.
    seasonal = 1.0 + 0.12 * np.cos(4 * np.pi * (doy - 15) / 365)
    return scale_mw * diurnal * seasonal


def _wind_solar_frame(
    asset: FleetAsset,
    weather: pl.DataFrame,
    rng: np.random.Generator,
) -> tuple[pl.DataFrame, np.ndarray]:
    """Compute a wind/solar asset's potential output; return (frame, potential)."""
    w = weather.filter(pl.col("asset_id") == asset.asset_id).sort("timestamp")
    ts = w["timestamp"]
    temp = w["temperature_c"].to_numpy()
    if asset.asset_type == WIND:
        assert asset.wind is not None
        potential = physics.wind_power_mwh(
            w["wind_speed_mps"].to_numpy(),
            temp,
            w["pressure_hpa"].to_numpy(),
            asset.capacity_mw,
            asset.wind,
            rng,
        )
    else:
        assert asset.solar is not None
        potential = physics.solar_power_mwh(
            w["ghi"].to_numpy(),
            temp,
            w["cloud_cover_pct"].to_numpy(),
            asset.capacity_mw,
            asset.solar,
            rng,
        )
    availability = np.clip(rng.normal(96.0, 3.0, len(ts)), 85.0, 100.0)
    frame = pl.DataFrame(
        {"timestamp": ts, "_potential": potential, "_availability": availability}
    )
    return frame, potential


def simulate_fleet(
    start_date: str,
    end_date: str,
    assets: tuple[FleetAsset, ...] = FLEET,
    use_real_weather: bool = True,
    random_seed: int = 42,
    warmup_days: int = 0,
    weather_seed: int = 42,
) -> SimulationResult:
    """Run the full hourly fleet simulation.

    Parameters
    ----------
    start_date, end_date : str
        ISO datetimes (inclusive) at hourly resolution.
    assets : tuple[FleetAsset, ...]
        Fleet to simulate. Defaults to :data:`FLEET`.
    use_real_weather : bool
        Attempt an Open-Meteo pull before falling back to synthetic weather.
    random_seed : int
        Seed for all stochastic components.
    warmup_days : int
        Simulate this many extra days before ``start_date`` and discard them from
        the returned frames. Lets state-carrying assets (battery SOC) and the
        dispatch rank signal reach a realistic trajectory before the target window.
    weather_seed : int
        Base seed for synthetic weather. Kept separate from ``random_seed`` so
        every caller shares one weather realization per calendar day regardless
        of their physics seed.

    Returns
    -------
    SimulationResult
        Generation frame, weather frame, and weather provenance.
    """
    start_dt = datetime.fromisoformat(start_date)
    sim_start = (start_dt - timedelta(days=warmup_days)).isoformat()

    rng = np.random.default_rng(random_seed)
    weather, source = get_weather(
        assets, sim_start, end_date, use_real_weather, weather_seed
    )

    timestamps = weather.select("timestamp").unique().sort("timestamp")["timestamp"]
    n = len(timestamps)
    ts_index = {t: i for i, t in enumerate(timestamps.to_list())}

    wind_solar = [a for a in assets if a.asset_type in (WIND, SOLAR)]
    batteries = [a for a in assets if a.asset_type == BATTERY]
    gas_units = [a for a in assets if a.asset_type == GAS]

    # --- 1. Wind + solar potential, aligned on the shared time axis ---
    potentials: dict[str, np.ndarray] = {}
    availabilities: dict[str, np.ndarray] = {}
    total_renewable = np.zeros(n)
    for asset in wind_solar:
        frame, potential = _wind_solar_frame(asset, weather, rng)
        idx = np.array([ts_index[t] for t in frame["timestamp"].to_list()])
        aligned = np.zeros(n)
        aligned[idx] = potential
        avail = np.full(n, 96.0)
        avail[idx] = frame["_availability"].to_numpy()
        potentials[asset.asset_id] = aligned
        availabilities[asset.asset_id] = avail
        total_renewable += aligned

    # --- 2. Demand + net load ---
    supply_scale = sum(a.capacity_mw for a in wind_solar + gas_units) or 1.0
    demand = _demand_curve(timestamps, supply_scale * 0.55)
    net_load_after_re = demand - total_renewable

    # --- 3. Battery dispatch against net load ---
    battery_signal = _rank_signal(net_load_after_re)
    battery_out: dict[str, dict[str, np.ndarray]] = {}
    total_battery_net = np.zeros(n)
    for asset in batteries:
        assert asset.battery is not None
        out = physics.battery_dispatch(
            battery_signal,
            asset.capacity_mw,
            asset.battery,
            charge_threshold=0.35,
            discharge_threshold=0.65,
        )
        battery_out[asset.asset_id] = out
        total_battery_net += out["net_mwh"]

    # --- 4. Gas dispatch against residual after storage ---
    net_load_after_batt = net_load_after_re - total_battery_net
    gas_signal = _rank_signal(net_load_after_batt)
    gas_out: dict[str, dict[str, np.ndarray]] = {}
    for asset in gas_units:
        assert asset.gas is not None
        threshold = 0.35 if asset.gas.subtype == "ccgt" else 0.80
        gas_out[asset.asset_id] = physics.gas_dispatch(
            gas_signal,
            asset.capacity_mw,
            asset.gas,
            threshold,
            rng,
        )

    # --- 5. Renewable curtailment on oversupply ---
    oversupply = np.clip(total_renewable - demand, 0.0, None)
    curtail_share = np.where(total_renewable > 0, oversupply / total_renewable, 0.0)

    rows: list[pl.DataFrame] = []
    ts_list = timestamps

    for asset in wind_solar:
        potential = potentials[asset.asset_id]
        curtailment = potential * curtail_share
        net = np.clip(potential - curtailment, 0.0, None)
        rows.append(
            _core_frame(
                ts_list,
                asset,
                gross=potential,
                net=net,
                curtailment=curtailment,
                availability=availabilities[asset.asset_id],
            )
        )

    for asset in batteries:
        out = battery_out[asset.asset_id]
        rows.append(
            _core_frame(
                ts_list,
                asset,
                gross=out["discharge_mwh"],
                net=out["net_mwh"],
                curtailment=np.zeros(n),
                availability=np.full(n, 98.0),
                soc_pct=out["soc_pct"],
                charge=out["charge_mwh"],
                discharge=out["discharge_mwh"],
            )
        )

    for asset in gas_units:
        out = gas_out[asset.asset_id]
        rows.append(
            _core_frame(
                ts_list,
                asset,
                gross=out["net_mwh"],
                net=out["net_mwh"],
                curtailment=np.zeros(n),
                availability=85.0 + 15.0 * out["online"],
                fuel=out["fuel_mmbtu"],
                heat_rate=out["heat_rate_btu_kwh"],
                co2=out["co2_tonnes"],
            )
        )

    generation = (
        pl.concat(rows).select(GENERATION_COLUMNS).sort(["timestamp", "asset_id"])
    )
    if warmup_days > 0:
        generation = generation.filter(pl.col("timestamp") >= start_dt)
        weather = weather.filter(pl.col("timestamp") >= start_dt)
    return SimulationResult(generation, weather, source)


def _core_frame(
    timestamps: pl.Series,
    asset: FleetAsset,
    *,
    gross: np.ndarray,
    net: np.ndarray,
    curtailment: np.ndarray,
    availability: np.ndarray,
    soc_pct: np.ndarray | None = None,
    charge: np.ndarray | None = None,
    discharge: np.ndarray | None = None,
    fuel: np.ndarray | None = None,
    heat_rate: np.ndarray | None = None,
    co2: np.ndarray | None = None,
) -> pl.DataFrame:
    """Assemble one asset's rows in the unified schema (nulls where N/A)."""
    n = len(timestamps)
    nulls: list[float | None] = [None] * n

    def _opt(arr: np.ndarray | None) -> list[float | None]:
        return nulls if arr is None else [float(x) for x in arr]

    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "asset_id": [asset.asset_id] * n,
            "asset_type": [asset.asset_type] * n,
            "gross_generation_mwh": gross.tolist(),
            "net_generation_mwh": net.tolist(),
            "curtailment_mwh": curtailment.tolist(),
            "availability_pct": availability.tolist(),
            "asset_capacity_mw": [asset.capacity_mw] * n,
            "soc_pct": _opt(soc_pct),
            "charge_mwh": _opt(charge),
            "discharge_mwh": _opt(discharge),
            "fuel_mmbtu": _opt(fuel),
            "heat_rate_btu_kwh": _opt(heat_rate),
            "co2_tonnes": _opt(co2),
        },
        schema_overrides={
            "soc_pct": pl.Float64,
            "charge_mwh": pl.Float64,
            "discharge_mwh": pl.Float64,
            "fuel_mmbtu": pl.Float64,
            "heat_rate_btu_kwh": pl.Float64,
            "co2_tonnes": pl.Float64,
        },
    )


__all__ = ["GENERATION_COLUMNS", "SimulationResult", "simulate_fleet"]
