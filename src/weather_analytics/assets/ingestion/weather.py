"""Weather ingestion asset — loads parquet files into Snowflake RAW via dlt."""

import logging
from collections.abc import Iterator
from pathlib import Path

import dlt
import polars as pl
from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    asset,
)

from weather_analytics.resources.dlt_resource import DltIngestionResource

logger = logging.getLogger(__name__)


class WeatherIngestionConfig(Config):
    """Run-time configuration for the weather ingestion asset.

    Parameters
    ----------
    source_path : str
        Directory containing ``weather_*.parquet`` files.
    """

    source_path: str = "data/raw/weather"


@dlt.resource(
    name="weather",
    write_disposition="merge",
    primary_key=["asset_id", "timestamp"],
)
def _weather_dlt_resource(
    source_path: str,
) -> Iterator[dict[str, object]]:
    """dlt resource that reads weather parquet files and yields records.

    Parameters
    ----------
    source_path : str
        Directory containing ``weather_*.parquet`` files.

    Yields
    ------
    dict[str, object]
        Individual weather records.
    """
    parquet_dir = Path(source_path)
    parquet_files = sorted(parquet_dir.glob("weather_*.parquet"))

    if not parquet_files:
        logger.warning("No weather parquet files found in %s", source_path)
        return

    total_rows = 0
    for file_path in parquet_files:
        logger.info("Reading %s", file_path.name)
        df = pl.read_parquet(file_path)
        records = df.to_dicts()
        total_rows += len(records)
        yield from records

    logger.info(
        "Read %d rows from %d weather parquet files",
        total_rows,
        len(parquet_files),
    )


@asset(
    name="waga_weather_ingestion",
    group_name="waga_ingestion",
    op_tags={"dagster/concurrency_key": "waga_ingestion"},
)
def waga_weather_ingestion(
    context: AssetExecutionContext,
    config: WeatherIngestionConfig,
    dlt_ingestion: DltIngestionResource,
) -> MaterializeResult:
    """Ingest weather parquet files into Snowflake RAW.weather via dlt.

    Uses ``write_disposition="merge"`` on composite key
    ``(asset_id, timestamp)`` to support idempotent re-runs.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context.
    config : WeatherIngestionConfig
        Run-time config with ``source_path``.
    dlt_ingestion : DltIngestionResource
        Configured dlt resource providing Snowflake pipeline.

    Returns
    -------
    MaterializeResult
        Dagster result with load metadata.
    """
    pipeline = dlt_ingestion.create_pipeline()

    weather_data = _weather_dlt_resource(source_path=config.source_path)
    load_info = pipeline.run(weather_data)

    # Extract metadata for Dagster UI
    load_id = load_info.loads_ids[0] if load_info.loads_ids else "no_load_id"
    has_failed = load_info.has_failed_jobs

    # Count loaded rows and schema changes
    rows_loaded = (
        load_info.metrics.get("rows_loaded", 0) if hasattr(load_info, "metrics") else 0
    )
    if rows_loaded == 0:
        # Fallback: count from load packages
        for package in load_info.load_packages:
            for job in package.jobs.get("completed_jobs", []):
                rows_loaded += getattr(job, "rows_count", 0)
    schema_changes: list[str] = []
    for package in load_info.load_packages:
        schema_update = package.schema_update
        if schema_update:
            schema_changes.extend(list(schema_update.keys()))

    if has_failed:
        context.log.error("Weather ingestion had failed jobs")
        for package in load_info.load_packages:
            failed_jobs = package.jobs.get("failed_jobs", [])
            for job in failed_jobs:
                context.log.error(
                    "Failed: %s — %s",
                    job.job_file_path,
                    job.failed_message,
                )
    else:
        context.log.info("Weather ingestion completed: load_id=%s", load_id)

    return MaterializeResult(
        metadata={
            "load_id": load_id,
            "rows_loaded": rows_loaded,
            "has_failed_jobs": has_failed,
            "schema_changes": schema_changes,
        },
    )
