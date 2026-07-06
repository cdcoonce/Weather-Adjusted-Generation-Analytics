"""Generate mock weather data for the fleet sites.

Thin wrapper over :func:`weather_analytics.mock_data.weather_sources.synthetic_weather`,
producing the RAW-weather schema (no ``cloud_cover_pct``) for every asset in
:data:`weather_analytics.mock_data.fleet.FLEET`. Deterministic per seed so the
weather and generation ingestion assets stay consistent for a given partition.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from weather_analytics.mock_data.fleet import FLEET
from weather_analytics.mock_data.weather_sources import synthetic_weather

logger = logging.getLogger(__name__)

# RAW weather columns (excludes the simulation-only ``cloud_cover_pct``).
_RAW_WEATHER_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "asset_id",
    "wind_speed_mps",
    "wind_direction_deg",
    "ghi",
    "temperature_c",
    "pressure_hpa",
    "relative_humidity",
)


def generate_weather_data(
    start_date: str,
    end_date: str,
    asset_count: int | None = None,  # noqa: ARG001
    random_seed: int = 42,
) -> pl.DataFrame:
    """Generate realistic hourly weather for the full fleet.

    Parameters
    ----------
    start_date, end_date : str
        ISO-format datetimes (inclusive) at hourly resolution.
    asset_count : int | None
        Deprecated / ignored — the fleet is defined by ``fleet.FLEET``.
    random_seed : int
        Seed for reproducibility (must match the generation asset's seed to keep
        weather and generation consistent for the same partition).

    Returns
    -------
    pl.DataFrame
        Weather with columns ``timestamp``, ``asset_id``, ``wind_speed_mps``,
        ``wind_direction_deg``, ``ghi``, ``temperature_c``, ``pressure_hpa``,
        ``relative_humidity``.
    """
    logger.info(
        "Generating weather data from %s to %s for %d assets",
        start_date,
        end_date,
        len(FLEET),
    )
    df = synthetic_weather(FLEET, start_date, end_date, random_seed=random_seed)
    result = df.select(_RAW_WEATHER_COLUMNS)
    logger.info(
        "Weather data completed: %d rows, %d assets", result.height, len(FLEET)
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
