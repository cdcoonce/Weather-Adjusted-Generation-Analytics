"""Generate mock generation data for renewable energy assets.

Produces realistic hourly generation records with wind power curves,
solar PV output, curtailment, and availability patterns.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

ASSET_CONFIGS: dict[str, dict[str, float | str]] = {
    "ASSET_001": {"capacity_mw": 50.0, "type": "wind"},
    "ASSET_002": {"capacity_mw": 75.0, "type": "wind"},
    "ASSET_003": {"capacity_mw": 100.0, "type": "wind"},
    "ASSET_004": {"capacity_mw": 45.0, "type": "wind"},
    "ASSET_005": {"capacity_mw": 80.0, "type": "wind"},
    "ASSET_006": {"capacity_mw": 30.0, "type": "solar"},
    "ASSET_007": {"capacity_mw": 50.0, "type": "solar"},
    "ASSET_008": {"capacity_mw": 40.0, "type": "solar"},
    "ASSET_009": {"capacity_mw": 60.0, "type": "solar"},
    "ASSET_010": {"capacity_mw": 35.0, "type": "solar"},
}


def _wind_power_curve(wind_speed: np.ndarray, capacity_mw: float) -> np.ndarray:
    """Calculate wind turbine power output based on simplified power curve.

    Parameters
    ----------
    wind_speed : np.ndarray
        Wind speed values in m/s.
    capacity_mw : float
        Turbine nameplate capacity in MW.

    Returns
    -------
    np.ndarray
        Power output in MWh for each wind speed value.
    """
    power = np.zeros_like(wind_speed)

    # Wind turbine characteristic speeds (m/s): cut-in, rated, cut-out
    cut_in, rated, cut_out = 3, 12, 25

    mask_ramp = (wind_speed >= cut_in) & (wind_speed < rated)
    power[mask_ramp] = capacity_mw * ((wind_speed[mask_ramp] - cut_in) / 9) ** 3

    mask_rated = (wind_speed >= rated) & (wind_speed < cut_out)
    power[mask_rated] = capacity_mw

    power[wind_speed >= cut_out] = 0
    return power


def _solar_power_output(ghi: np.ndarray, capacity_mw: float) -> np.ndarray:
    """Calculate solar PV power output based on irradiance.

    Parameters
    ----------
    ghi : np.ndarray
        Global horizontal irradiance values in W/m^2.
    capacity_mw : float
        PV nameplate capacity in MW.

    Returns
    -------
    np.ndarray
        Power output in MWh for each irradiance value.
    """
    efficiency = 0.85
    power = (ghi / 1000) * capacity_mw * efficiency
    return np.clip(power, 0, capacity_mw)


def generate_generation_data(
    start_date: str,
    end_date: str,
    asset_configs: dict[str, dict[str, float | str]] | None = None,
    random_seed: int = 42,
) -> pl.DataFrame:
    """Generate realistic hourly generation data for renewable energy assets.

    Parameters
    ----------
    start_date : str
        ISO-format start datetime (inclusive).
    end_date : str
        ISO-format end datetime (inclusive).
    asset_configs : dict[str, dict[str, float | str]] | None
        Mapping of asset_id to config dict with ``capacity_mw`` and ``type``
        keys.  Defaults to :data:`ASSET_CONFIGS`.
    random_seed : int
        Numpy random seed for reproducibility.

    Returns
    -------
    pl.DataFrame
        Generation DataFrame with columns: ``timestamp``, ``asset_id``,
        ``gross_generation_mwh``, ``net_generation_mwh``,
        ``curtailment_mwh``, ``availability_pct``, ``asset_capacity_mw``.
    """
    if asset_configs is None:
        asset_configs = ASSET_CONFIGS

    logger.info(
        "Generating generation data from %s to %s for %d assets",
        start_date,
        end_date,
        len(asset_configs),
    )

    rng = np.random.default_rng(random_seed)

    timestamps = pl.datetime_range(
        start=datetime.fromisoformat(start_date),
        end=datetime.fromisoformat(end_date),
        interval="1h",
        eager=True,
    )

    asset_ids = list(asset_configs.keys())
    base_df = pl.DataFrame({"timestamp": timestamps}).join(
        pl.DataFrame({"asset_id": asset_ids}),
        how="cross",
    )

    n_rows = len(base_df)

    # Build synthetic weather signals for generation correlation
    df = base_df.with_columns(
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
    )

    hour_array = df["hour"].to_numpy()
    day_of_year = df["day_of_year"].to_numpy()

    # Synthetic wind speed
    seasonal_wind = 10 + 3 * np.sin(2 * np.pi * day_of_year / 365)
    diurnal_wind = 1.5 * np.sin(2 * np.pi * (hour_array - 6) / 24)
    wind_noise = rng.normal(0, 2, n_rows)
    wind_speed = np.clip(seasonal_wind + diurnal_wind + wind_noise, 0, 25)

    # Synthetic GHI
    max_ghi = 900 + 100 * np.sin(2 * np.pi * (day_of_year - 80) / 365)
    solar_hour = hour_array - 12
    daylight_half_span = 6  # hours from solar noon with nonzero GHI
    ghi_pattern = np.where(
        np.abs(solar_hour) < daylight_half_span,
        max_ghi * (1 - (solar_hour / 8) ** 2),
        0,
    )
    cloud_factor = rng.uniform(0.7, 1.0, n_rows)
    ghi = np.clip(ghi_pattern * cloud_factor, 0, 1000)

    # Compute gross generation per asset type
    asset_id_list = df["asset_id"].to_list()
    gross_generation = np.zeros(n_rows)
    asset_capacity = np.zeros(n_rows)

    for i, asset_id_val in enumerate(asset_id_list):
        cfg = asset_configs[asset_id_val]
        capacity = float(cfg["capacity_mw"])
        asset_type = cfg["type"]
        asset_capacity[i] = capacity

        if asset_type == "wind":
            gross_generation[i] = _wind_power_curve(
                np.array([wind_speed[i]]), capacity
            )[0]
        else:
            gross_generation[i] = _solar_power_output(np.array([ghi[i]]), capacity)[0]

    # Performance and loss factors
    performance_factor = rng.uniform(0.95, 1.0, n_rows)
    gross_generation = gross_generation * performance_factor

    loss_factor = rng.uniform(0.90, 0.95, n_rows)
    net_generation = gross_generation * loss_factor

    # Curtailment
    curtailment_probability = gross_generation / (asset_capacity + 1e-6)
    curtailment_mask = rng.random(n_rows) < (curtailment_probability * 0.1)
    curtailment = np.where(
        curtailment_mask,
        rng.uniform(0.05, 0.15, n_rows) * gross_generation,
        0,
    )
    net_generation = np.maximum(net_generation - curtailment, 0)

    # Availability
    base_availability = 95.0
    availability_variation = rng.normal(0, 5, n_rows)
    availability = np.clip(base_availability + availability_variation, 85, 100)

    availability_factor = availability / 100
    gross_generation = gross_generation * availability_factor
    net_generation = net_generation * availability_factor

    result = (
        df.select(["timestamp", "asset_id"])
        .with_columns(
            pl.Series("gross_generation_mwh", gross_generation),
            pl.Series("net_generation_mwh", net_generation),
            pl.Series("curtailment_mwh", curtailment),
            pl.Series("availability_pct", availability),
            pl.Series("asset_capacity_mw", asset_capacity),
        )
        .sort(["timestamp", "asset_id"])
    )

    logger.info(
        "Generation data generation completed: %d rows, %d assets",
        len(result),
        len(asset_configs),
    )
    return result


def save_generation_parquet(
    df: pl.DataFrame,
    output_dir: Path,
    partition_by_date: bool = True,
) -> Path:
    """Save generation data to Parquet files.

    Parameters
    ----------
    df : pl.DataFrame
        Generation DataFrame to persist.
    output_dir : Path
        Directory for output parquet files.
    partition_by_date : bool
        If True, write one file per date. Otherwise write a single file.

    Returns
    -------
    Path
        The output directory containing the written files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if partition_by_date:
        dates = (
            df.select(pl.col("timestamp").dt.date().alias("date")).unique().sort("date")
        )
        logger.info(
            "Saving %d daily generation files to %s",
            len(dates),
            output_dir,
        )

        for (date_val,) in dates.iter_rows():
            date_str = date_val.isoformat()
            daily_df = df.filter(pl.col("timestamp").dt.date() == date_val)
            output_path = output_dir / f"generation_{date_str}.parquet"
            daily_df.write_parquet(output_path, compression="snappy")

        logger.info("Saved %d generation files", len(dates))
    else:
        output_path = output_dir / "generation_all.parquet"
        df.write_parquet(output_path, compression="snappy")
        logger.info("Saved generation data to %s", output_path)

    return output_dir


__all__ = [
    "ASSET_CONFIGS",
    "generate_generation_data",
    "save_generation_parquet",
]
