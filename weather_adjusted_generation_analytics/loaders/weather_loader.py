"""Weather data loader using dlt for DuckDB ingestion."""

from pathlib import Path
from typing import Iterator

import dlt
import polars as pl

from weather_adjusted_generation_analytics.config import config
from weather_adjusted_generation_analytics.utils import get_logger

logger = get_logger(__name__)


@dlt.resource(
    name="weather",
    write_disposition="merge",
    primary_key=["asset_id", "timestamp"],
)
def load_weather_parquet(
    file_paths: list[Path] | None = None,
) -> Iterator[dict]:
    """
    Load weather data from Parquet files for dlt ingestion.

    Parameters
    ----------
    file_paths : list[Path], optional
        List of Parquet file paths to load. If None, loads all files
        from the configured weather directory.

    Yields
    ------
    dict
        Weather record as dictionary

    """
    if file_paths is None:
        file_paths = sorted(config.weather_raw_path.glob("weather_*.parquet"))

    logger.info(f"Loading {len(file_paths)} weather Parquet files")

    total_rows = 0

    for file_path in file_paths:
        try:
            logger.info(f"Reading {file_path.name}")
            df = pl.read_parquet(file_path)
            records = df.to_dicts()
            total_rows += len(records)

            for record in records:
                yield record

        except Exception as exc:
            logger.error(f"Error reading {file_path}: {exc}", exc_info=True)
            raise

    logger.info(
        "Completed loading weather data",
        extra={
            "extra_fields": {
                "files_processed": len(file_paths),
                "total_rows": total_rows,
            }
        },
    )


def get_weather_pipeline() -> dlt.Pipeline:
    """Create and configure dlt pipeline for weather data ingestion."""
    return dlt.pipeline(
        pipeline_name=f"{config.dlt_pipeline_name}_weather",
        destination=dlt.destinations.duckdb(
            credentials=str(config.duckdb_path),
        ),
        dataset_name=config.dlt_schema,
        progress="log",
    )


def run_weather_ingestion(file_paths: list[Path] | None = None) -> None:
    """
    Execute weather data ingestion pipeline.

    Parameters
    ----------
    file_paths : list[Path], optional
        Specific files to ingest. If None, ingests all files.

    Returns
    -------
    None

    """
    logger.info("Starting weather data ingestion")

    try:
        pipeline = get_weather_pipeline()
        load_info = pipeline.run(
            load_weather_parquet(file_paths=file_paths),
            loader_file_format="jsonl",
        )

        logger.info(
            f"Weather ingestion completed: {load_info}",
            extra={
                "extra_fields": {
                    "pipeline_name": pipeline.pipeline_name,
                    "destination": str(config.duckdb_path),
                }
            },
        )

        if load_info.has_failed_jobs:
            logger.error("Weather ingestion had failures")
            for package in load_info.load_packages:
                for job in package.jobs["failed_jobs"]:
                    logger.error(
                        f"Failed job: {job.job_file_path} - {job.failed_message}"
                    )
        else:
            logger.info("All weather data loaded successfully")

    except Exception as exc:
        logger.error(f"Weather ingestion failed: {exc}", exc_info=True)
        raise


__all__ = [
    "get_weather_pipeline",
    "load_weather_parquet",
    "run_weather_ingestion",
]

