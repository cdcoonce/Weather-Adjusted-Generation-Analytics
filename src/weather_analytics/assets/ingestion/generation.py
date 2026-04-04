"""Generation ingestion asset — loads parquet files into Snowflake RAW via dlt."""

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


class GenerationIngestionConfig(Config):
    """Run-time configuration for the generation ingestion asset.

    Parameters
    ----------
    source_path : str
        Directory containing ``generation_*.parquet`` files.
    """

    source_path: str = "data/raw/generation"


@dlt.resource(
    name="generation",
    write_disposition="merge",
    primary_key=["asset_id", "timestamp"],
)
def _generation_dlt_resource(
    source_path: str,
) -> Iterator[dict[str, object]]:
    """dlt resource that reads generation parquet files and yields records.

    Parameters
    ----------
    source_path : str
        Directory containing ``generation_*.parquet`` files.

    Yields
    ------
    dict[str, object]
        Individual generation records.
    """
    parquet_dir = Path(source_path)
    parquet_files = sorted(parquet_dir.glob("generation_*.parquet"))

    if not parquet_files:
        logger.warning("No generation parquet files found in %s", source_path)
        return

    total_rows = 0
    for file_path in parquet_files:
        logger.info("Reading %s", file_path.name)
        df = pl.read_parquet(file_path)
        records = df.to_dicts()
        total_rows += len(records)
        yield from records

    logger.info(
        "Read %d rows from %d generation parquet files",
        total_rows,
        len(parquet_files),
    )


@asset(
    name="waga_generation_ingestion",
    group_name="waga_ingestion",
    op_tags={"dagster/concurrency_key": "waga_ingestion"},
)
def waga_generation_ingestion(
    context: AssetExecutionContext,
    config: GenerationIngestionConfig,
    dlt_ingestion: DltIngestionResource,
) -> MaterializeResult:
    """Ingest generation parquet files into Snowflake RAW.generation via dlt.

    Uses ``write_disposition="merge"`` on composite key
    ``(asset_id, timestamp)`` to support idempotent re-runs.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context.
    config : GenerationIngestionConfig
        Run-time config with ``source_path``.
    dlt_ingestion : DltIngestionResource
        Configured dlt resource providing Snowflake pipeline.

    Returns
    -------
    MaterializeResult
        Dagster result with load metadata.
    """
    pipeline = dlt_ingestion.create_pipeline()

    generation_data = _generation_dlt_resource(source_path=config.source_path)
    load_info = pipeline.run(generation_data)

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
        context.log.error("Generation ingestion had failed jobs")
        for package in load_info.load_packages:
            failed_jobs = package.jobs.get("failed_jobs", [])
            for job in failed_jobs:
                context.log.error(
                    "Failed: %s — %s",
                    job.job_file_path,
                    job.failed_message,
                )
    else:
        context.log.info("Generation ingestion completed: load_id=%s", load_id)

    return MaterializeResult(
        metadata={
            "load_id": load_id,
            "rows_loaded": rows_loaded,
            "has_failed_jobs": has_failed,
            "schema_changes": schema_changes,
        },
    )
