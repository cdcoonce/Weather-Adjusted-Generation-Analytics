"""Generate mock weather data for renewable energy assets.

Produces realistic hourly weather observations with seasonal and diurnal
patterns for wind, solar irradiance, temperature, pressure, and humidity.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)


def generate_weather_data(
    start_date: str,
    end_date: str,
    asset_count: int,
    random_seed: int = 42,
) -> pl.DataFrame:
    """Generate realistic hourly weather data for multiple assets.

    Parameters
    ----------
    start_date : str
        ISO-format start datetime (inclusive).
    end_date : str
        ISO-format end datetime (inclusive).
    asset_count : int
        Number of simulated asset sites.
    random_seed : int
        Numpy random seed for reproducibility.

    Returns
    -------
    pl.DataFrame
        Weather DataFrame with columns: ``timestamp``, ``asset_id``,
        ``wind_speed_mps``, ``wind_direction_deg``, ``ghi``,
        ``temperature_c``, ``pressure_hpa``, ``relative_humidity``.
    """
    logger.info(
        "Generating weather data from %s to %s for %d assets",
        start_date,
        end_date,
        asset_count,
    )

    rng = np.random.default_rng(random_seed)

    timestamps = pl.datetime_range(
        start=datetime.fromisoformat(start_date),
        end=datetime.fromisoformat(end_date),
        interval="1h",
        eager=True,
    )

    asset_ids = [f"ASSET_{str(i + 1).zfill(3)}" for i in range(asset_count)]

    base_df = pl.DataFrame({"timestamp": timestamps}).join(
        pl.DataFrame({"asset_id": asset_ids}),
        how="cross",
    )

    n_rows = len(base_df)

    df = base_df.with_columns(
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
    )

    hour_array = df["hour"].to_numpy()
    day_of_year = df["day_of_year"].to_numpy()

    # Wind speed: seasonal + diurnal + noise, clipped to [0, 25]
    seasonal_wind = 10 + 3 * np.sin(2 * np.pi * day_of_year / 365)
    diurnal_wind = 1.5 * np.sin(2 * np.pi * (hour_array - 6) / 24)
    wind_noise = rng.normal(0, 2, n_rows)
    wind_speed = np.clip(seasonal_wind + diurnal_wind + wind_noise, 0, 25)

    # Wind direction: asset-specific base + noise, mod 360
    asset_base_direction = {asset: rng.uniform(0, 360) for asset in asset_ids}
    base_direction = np.array(
        [asset_base_direction[a] for a in df["asset_id"].to_list()]
    )
    direction_change = rng.normal(0, 20, n_rows)
    wind_direction = (base_direction + direction_change) % 360

    # Solar GHI: bell curve centered at noon, seasonal amplitude
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

    # Temperature: seasonal + diurnal + noise
    seasonal_temp = 15 + 12 * np.sin(2 * np.pi * (day_of_year - 80) / 365)
    diurnal_temp = 5 * np.sin(2 * np.pi * (hour_array - 6) / 24)
    temp_noise = rng.normal(0, 2, n_rows)
    temperature = seasonal_temp + diurnal_temp + temp_noise

    # Pressure: seasonal + noise
    base_pressure = 1013
    seasonal_pressure = 5 * np.sin(2 * np.pi * day_of_year / 365)
    pressure_noise = rng.normal(0, 5, n_rows)
    pressure = base_pressure + seasonal_pressure + pressure_noise

    # Relative humidity: base - temp effect - ghi effect + noise
    base_humidity = 65
    temp_effect = -0.5 * (temperature - 15)
    ghi_effect = -0.01 * ghi
    humidity_noise = rng.normal(0, 10, n_rows)
    relative_humidity = np.clip(
        base_humidity + temp_effect + ghi_effect + humidity_noise,
        20,
        95,
    )

    result = (
        df.select(["timestamp", "asset_id"])
        .with_columns(
            pl.Series("wind_speed_mps", wind_speed),
            pl.Series("wind_direction_deg", wind_direction),
            pl.Series("ghi", ghi),
            pl.Series("temperature_c", temperature),
            pl.Series("pressure_hpa", pressure),
            pl.Series("relative_humidity", relative_humidity),
        )
        .sort(["timestamp", "asset_id"])
    )

    logger.info(
        "Weather data generation completed: %d rows, %d assets",
        len(result),
        asset_count,
    )
    return result


def save_weather_parquet(
    df: pl.DataFrame,
    output_dir: Path,
    partition_by_date: bool = True,
) -> Path:
    """Save weather data to Parquet files.

    Parameters
    ----------
    df : pl.DataFrame
        Weather DataFrame to persist.
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
        logger.info("Saving %d daily weather files to %s", len(dates), output_dir)

        for (date_val,) in dates.iter_rows():
            date_str = date_val.isoformat()
            daily_df = df.filter(pl.col("timestamp").dt.date() == date_val)
            output_path = output_dir / f"weather_{date_str}.parquet"
            daily_df.write_parquet(output_path, compression="snappy")

        logger.info("Saved %d weather files", len(dates))
    else:
        output_path = output_dir / "weather_all.parquet"
        df.write_parquet(output_path, compression="snappy")
        logger.info("Saved weather data to %s", output_path)

    return output_dir


__all__ = ["generate_weather_data", "save_weather_parquet"]
