"""Build the dashboard JSON exports locally, without Snowflake.

This mirrors the aggregation math of the dbt marts + the Dagster
``waga_dashboard_export_build`` asset, but runs entirely on in-memory Polars
frames from :func:`weather_analytics.mock_data.simulate.simulate_fleet`. It is
what lets the multi-technology fleet (wind, solar, battery, gas) reach the
static dashboard on a laptop or in CI with no warehouse credentials.

Weather-adjusted performance (expected-vs-actual regression) is computed for
wind and solar exactly as the mart does. Battery and gas are not weather-driven,
so they get technology-appropriate scores (storage round-trip efficiency; gas
part-load efficiency) and carry extra columns (SOC, throughput, fuel, CO2).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from weather_analytics.mock_data.fleet import (
    BATTERY,
    FLEET,
    GAS,
    SOLAR,
    WIND,
    FleetAsset,
)
from weather_analytics.mock_data.simulate import SimulationResult, simulate_fleet

SCHEMA_VERSION = "2.0"

_RATING_HIGH = 0.6
_RATING_MEDIUM = 0.3


@dataclass(frozen=True)
class ExportBundle:
    """The four JSON payloads (as Python objects) plus the manifest."""

    manifest: dict[str, Any]
    assets: list[dict[str, Any]]
    daily: list[dict[str, Any]]
    weather: list[dict[str, Any]]


def _linfit(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    """Least-squares slope, intercept, and R^2 of y on x."""
    if x.size < 2 or np.allclose(x, x[0]):
        return 0.0, float(np.mean(y)) if y.size else 0.0, 0.0
    slope, intercept = np.polyfit(x, y, 1)
    pred = slope * x + intercept
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 0.0 if ss_tot == 0 else max(0.0, 1.0 - ss_res / ss_tot)
    return float(slope), float(intercept), r2


def _hourly_rating(cf: np.ndarray) -> np.ndarray:
    """Per-hour performance bucket from capacity factor (mirrors stg_generation)."""
    return np.select(
        [cf > 0.9, cf > 0.6, cf > 0.3, cf > 0.05],
        ["Excellent", "Good", "Fair", "Poor"],
        default="Very Poor",
    )


def _daily_frame(gen: pl.DataFrame, weather: pl.DataFrame) -> pl.DataFrame:
    """Roll hourly generation + weather up to per-asset-per-day rows."""
    gen = gen.with_columns(
        pl.col("timestamp").dt.date().alias("date"),
        (pl.col("net_generation_mwh") / pl.col("asset_capacity_mw")).alias(
            "_hourly_cf"
        ),
    )
    ratings = _hourly_rating(gen["_hourly_cf"].to_numpy())
    gen = gen.with_columns(pl.Series("_rating", ratings))

    daily_gen = gen.group_by(["asset_id", "date"]).agg(
        pl.col("asset_type").first(),
        pl.col("asset_capacity_mw").max(),
        pl.col("gross_generation_mwh").sum().alias("total_gross_generation_mwh"),
        pl.col("net_generation_mwh").sum().alias("total_net_generation_mwh"),
        pl.col("curtailment_mwh").sum().alias("total_curtailment_mwh"),
        pl.col("availability_pct").mean().alias("avg_availability_pct"),
        pl.len().alias("_hours"),
        (pl.col("_rating") == "Excellent").sum().alias("excellent_hours"),
        (pl.col("_rating") == "Good").sum().alias("good_hours"),
        (pl.col("_rating") == "Fair").sum().alias("fair_hours"),
        (pl.col("_rating") == "Poor").sum().alias("poor_hours"),
        pl.col("soc_pct").mean().alias("avg_soc_pct"),
        pl.col("charge_mwh").sum().alias("total_charge_mwh"),
        pl.col("discharge_mwh").sum().alias("total_discharge_mwh"),
        pl.col("fuel_mmbtu").sum().alias("total_fuel_mmbtu"),
        pl.col("heat_rate_btu_kwh").mean().alias("avg_heat_rate_btu_kwh"),
        pl.col("co2_tonnes").sum().alias("total_co2_tonnes"),
    )

    daily_weather = (
        weather.with_columns(pl.col("timestamp").dt.date().alias("date"))
        .group_by(["asset_id", "date"])
        .agg(
            pl.col("wind_speed_mps").mean().alias("avg_wind_speed_mps"),
            pl.col("ghi").mean().alias("avg_ghi"),
            pl.col("temperature_c").mean().alias("avg_temperature_c"),
        )
    )

    daily = daily_gen.join(daily_weather, on=["asset_id", "date"], how="left")
    daily = daily.with_columns(
        (
            pl.col("total_net_generation_mwh")
            / (pl.col("asset_capacity_mw") * pl.col("_hours"))
        ).alias("daily_capacity_factor"),
        pl.lit(100.0).alias("data_completeness_pct"),
    )
    daily = daily.with_columns(
        pl.when(pl.col("daily_capacity_factor") >= _RATING_HIGH)
        .then(pl.lit("High"))
        .when(pl.col("daily_capacity_factor") >= _RATING_MEDIUM)
        .then(pl.lit("Medium"))
        .otherwise(pl.lit("Low"))
        .alias("daily_performance_rating")
    )
    return daily.sort(["asset_id", "date"])


def _weather_performance(
    gen: pl.DataFrame,
    weather: pl.DataFrame,
    daily: pl.DataFrame,
    fleet_by_id: dict[str, FleetAsset],
) -> pl.DataFrame:
    """Per-asset-per-day performance scores (weather-adjusted for wind/solar)."""
    joined = gen.join(
        weather.select(["timestamp", "asset_id", "wind_speed_mps", "ghi"]),
        on=["timestamp", "asset_id"],
        how="left",
    ).with_columns(pl.col("timestamp").dt.date().alias("date"))

    frames: list[pl.DataFrame] = []
    for asset_id, asset in fleet_by_id.items():
        a = joined.filter(pl.col("asset_id") == asset_id).sort("timestamp")
        if a.height == 0:
            continue
        net = a["net_generation_mwh"].to_numpy()
        dates = a["date"]

        if asset.asset_type in (WIND, SOLAR):
            driver = (
                (a["wind_speed_mps"] if asset.asset_type == WIND else a["ghi"])
                .fill_null(0.0)
                .to_numpy()
            )
            slope, intercept, r2 = _linfit(driver, net)
            expected = np.clip(slope * driver + intercept, 0.0, None)
            # Only score "operating" hours: at night / dead-calm the expected
            # output is ~0, so the actual/expected ratio is undefined noise that
            # would otherwise drag the daily score toward zero.
            operating = expected > 0.02 * asset.capacity_mw
            ratio = np.divide(
                net * 100.0,
                expected,
                out=np.full(net.shape, np.nan),
                where=operating,
            )
            wind_r2 = r2 if asset.asset_type == WIND else None
            solar_r2 = r2 if asset.asset_type == SOLAR else None
        else:
            # Non-weather-driven: score on operational efficiency instead.
            expected = net.copy()
            ratio = _operational_ratio(asset, a)
            wind_r2 = None
            solar_r2 = None

        per_hour = pl.DataFrame(
            {
                "date": dates,
                "_expected": expected,
                "_actual": net,
                "_ratio": ratio,
            }
        )
        agg = per_hour.group_by("date").agg(
            pl.col("_expected").mean().alias("avg_expected_generation_mwh"),
            pl.col("_actual").mean().alias("avg_actual_generation_mwh"),
            # drop_nans so non-operating hours (night / calm) don't poison the
            # daily mean via NaN propagation.
            pl.col("_ratio").drop_nans().mean().alias("avg_performance_ratio_pct"),
        )
        agg = agg.with_columns(
            pl.lit(asset_id).alias("asset_id"),
            pl.lit(asset.asset_type).alias("inferred_asset_type"),
            pl.lit(wind_r2, dtype=pl.Float64).alias("wind_r_squared"),
            pl.lit(solar_r2, dtype=pl.Float64).alias("solar_r_squared"),
            pl.col("avg_performance_ratio_pct")
            .clip(0.0, 100.0)
            .alias("performance_score"),
        )
        agg = agg.with_columns(
            pl.when(pl.col("performance_score") >= 95)
            .then(pl.lit("Excellent"))
            .when(pl.col("performance_score") >= 85)
            .then(pl.lit("Good"))
            .when(pl.col("performance_score") >= 70)
            .then(pl.lit("Fair"))
            .otherwise(pl.lit("Poor"))
            .alias("performance_category")
        )
        frames.append(agg)

    weather_perf = pl.concat(frames)

    # Rolling 7d / 30d capacity factor from the daily frame.
    rolling = (
        daily.select(["asset_id", "date", "daily_capacity_factor"])
        .sort(["asset_id", "date"])
        .with_columns(
            pl.col("daily_capacity_factor")
            .rolling_mean(window_size=7, min_samples=1)
            .over("asset_id")
            .alias("rolling_7d_avg_cf"),
            pl.col("daily_capacity_factor")
            .rolling_mean(window_size=30, min_samples=1)
            .over("asset_id")
            .alias("rolling_30d_avg_cf"),
        )
        .drop("daily_capacity_factor")
    )

    return weather_perf.join(rolling, on=["asset_id", "date"], how="left").sort(
        ["asset_id", "date"]
    )


def _operational_ratio(asset: FleetAsset, hourly: pl.DataFrame) -> np.ndarray:
    """Technology performance ratio (%) for non-weather assets, per hour.

    Battery: realized round-trip efficiency when discharging. Gas: full-load
    heat rate as a fraction of the actual (part-load-penalized) heat rate.
    """
    n = hourly.height
    if asset.asset_type == BATTERY:
        discharge = hourly["discharge_mwh"].fill_null(0.0).to_numpy()
        assert asset.battery is not None
        # 100 when delivering at rated round-trip efficiency; scale by activity.
        active = discharge > 1e-6
        ratio = np.full(n, np.nan)
        ratio[active] = asset.battery.round_trip_efficiency * 100.0
        return ratio
    if asset.asset_type == GAS:
        assert asset.gas is not None
        hr = hourly["heat_rate_btu_kwh"].fill_null(asset.gas.heat_rate_btu_kwh)
        out = hourly["net_generation_mwh"].to_numpy()
        hr_np = hr.to_numpy()
        ratio = np.full(n, np.nan)
        running = out > 1e-6
        ratio[running] = np.clip(
            asset.gas.heat_rate_btu_kwh / hr_np[running] * 100.0, 0.0, 100.0
        )
        return ratio
    return np.full(n, np.nan)


def _assets_frame(fleet: tuple[FleetAsset, ...]) -> list[dict[str, Any]]:
    """Build the asset dimension records for assets.json."""
    return [
        {
            "asset_id": a.asset_id,
            "capacity_mw": a.capacity_mw,
            "size_category": a.size_category,
            "asset_type": a.asset_type,
            "display_name": a.display_name,
            "name": a.name,
            "region": a.region,
            "latitude": a.latitude,
            "longitude": a.longitude,
        }
        for a in fleet
    ]


def _records(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Serialize a frame to JSON records, dates as ISO strings, NaN -> None."""
    casts = [
        pl.col(c).cast(pl.Utf8) if df[c].dtype.is_temporal() else pl.col(c)
        for c in df.columns
    ]
    return df.select(casts).fill_nan(None).to_dicts()


def build_bundle(
    result: SimulationResult, fleet: tuple[FleetAsset, ...]
) -> ExportBundle:
    """Compute all four export payloads from a simulation result."""
    fleet_by_id = {a.asset_id: a for a in fleet}
    daily = _daily_frame(result.generation, result.weather)
    weather_perf = _weather_performance(
        result.generation, result.weather, daily, fleet_by_id
    )

    daily_out = daily.select(
        "asset_id",
        "date",
        "total_net_generation_mwh",
        "daily_capacity_factor",
        "avg_availability_pct",
        "total_curtailment_mwh",
        "daily_performance_rating",
        "excellent_hours",
        "good_hours",
        "fair_hours",
        "poor_hours",
        "avg_wind_speed_mps",
        "avg_ghi",
        "avg_temperature_c",
        "data_completeness_pct",
        "asset_type",
        "avg_soc_pct",
        "total_charge_mwh",
        "total_discharge_mwh",
        "total_fuel_mmbtu",
        "avg_heat_rate_btu_kwh",
        "total_co2_tonnes",
    )
    weather_out = weather_perf.select(
        "asset_id",
        "date",
        "performance_score",
        "performance_category",
        "avg_expected_generation_mwh",
        "avg_actual_generation_mwh",
        "avg_performance_ratio_pct",
        "wind_r_squared",
        "solar_r_squared",
        "inferred_asset_type",
        "rolling_7d_avg_cf",
        "rolling_30d_avg_cf",
    )

    type_counts: dict[str, int] = {}
    for a in fleet:
        type_counts[a.asset_type] = type_counts.get(a.asset_type, 0) + 1

    manifest = {
        "generated_at": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "weather_source": result.weather_source,
        "date_range": {
            "start": str(daily["date"].min()),
            "end": str(daily["date"].max()),
        },
        "asset_count": len(fleet),
        "asset_type_counts": type_counts,
        "row_counts": {
            "daily_performance": daily_out.height,
            "weather_performance": weather_out.height,
        },
        "schema_version": SCHEMA_VERSION,
    }
    return ExportBundle(
        manifest=manifest,
        assets=_assets_frame(fleet),
        daily=_records(daily_out),
        weather=_records(weather_out),
    )


def write_bundle(bundle: ExportBundle, out_dir: Path) -> None:
    """Write the four JSON export files to ``out_dir``."""
    out_dir.mkdir(parents=True, exist_ok=True)
    payloads = {
        "manifest.json": bundle.manifest,
        "assets.json": bundle.assets,
        "daily_performance.json": bundle.daily,
        "weather_performance.json": bundle.weather,
    }
    for name, payload in payloads.items():
        (out_dir / name).write_text(
            json.dumps(payload, separators=(",", ":")), encoding="utf-8"
        )


def build_local_exports(
    start_date: str,
    end_date: str,
    out_dir: Path,
    fleet: tuple[FleetAsset, ...] = FLEET,
    use_real_weather: bool = True,
    random_seed: int = 42,
) -> dict[str, Any]:
    """Simulate the fleet and write the dashboard exports; return the manifest."""
    result = simulate_fleet(start_date, end_date, fleet, use_real_weather, random_seed)
    bundle = build_bundle(result, fleet)
    write_bundle(bundle, out_dir)
    return bundle.manifest


__all__ = [
    "SCHEMA_VERSION",
    "ExportBundle",
    "build_bundle",
    "build_local_exports",
    "write_bundle",
]
