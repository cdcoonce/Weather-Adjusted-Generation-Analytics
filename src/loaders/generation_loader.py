"""Generation data loader using dlt for DuckDB ingestion."""

from pathlib import Path
from typing import Iterator

import dlt
import polars as pl

from src.config import config
from src.utils import get_logger

logger = get_logger(__name__)


@dlt.resource(
    name="generation",
    write_disposition="merge",
    primary_key=["asset_id", "timestamp"],
)
def load_generation_parquet(
    file_paths: list[Path] | None = None,
) -> Iterator[dict]:
    """
    Load generation data from Parquet files for dlt ingestion.

    Reads Parquet files using Polars and yields rows as dictionaries
    for dlt to process. Supports incremental loading via merge
    write disposition with composite primary key.

    Parameters
    ----------
    file_paths : list[Path], optional
        List of Parquet file paths to load. If None, loads all files
        from the configured generation directory.

    Yields
    ------
    dict
        Generation record as dictionary

    Examples
    --------
    >>> pipeline = dlt.pipeline(
    ...     pipeline_name="generation_ingestion",
    ...     destination="duckdb",
    ...     dataset_name="renewable_energy"
    ... )
    >>> load_info = pipeline.run(load_generation_parquet())

    """
    if file_paths is None:
        file_paths = sorted(config.generation_raw_path.glob("generation_*.parquet"))

    logger.info(f"Loading {len(file_paths)} generation Parquet files")

    total_rows = 0

    for file_path in file_paths:
        try:
            logger.info(f"Reading {file_path.name}")

            # Read Parquet file with Polars
            df = pl.read_parquet(file_path)

            # Convert to records (list of dicts)
            records = df.to_dicts()

            total_rows += len(records)

            # Yield each record
            for record in records:
                yield record

        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}", exc_info=True)
            raise

    logger.info(
        f"Completed loading generation data",
        extra={
            "extra_fields": {
                "files_processed": len(file_paths),
                "total_rows": total_rows,
            }
        },
    )


def get_generation_pipeline() -> dlt.Pipeline:
    """
    Create and configure dlt pipeline for generation data ingestion.

    Returns
    -------
    dlt.Pipeline
        Configured dlt pipeline instance

    """
    pipeline = dlt.pipeline(
        pipeline_name=f"{config.dlt_pipeline_name}_generation",
        destination=dlt.destinations.duckdb(
            credentials=str(config.duckdb_path),
        ),
        dataset_name=config.dlt_schema,
        progress="log",
    )

    return pipeline


def run_generation_ingestion(file_paths: list[Path] | None = None) -> None:
    """
    Execute generation data ingestion pipeline.

    Loads generation data from Parquet files into DuckDB using dlt.

    Parameters
    ----------
    file_paths : list[Path], optional
        Specific files to ingest. If None, ingests all files.

    Returns
    -------
    None

    """
    logger.info("Starting generation data ingestion")

    try:
        # Get pipeline
        pipeline = get_generation_pipeline()

        # Run pipeline
        load_info = pipeline.run(
            load_generation_parquet(file_paths=file_paths),
            loader_file_format="jsonl",
        )

        logger.info(
            f"Generation ingestion completed: {load_info}",
            extra={
                "extra_fields": {
                    "pipeline_name": pipeline.pipeline_name,
                    "destination": str(config.duckdb_path),
                }
            },
        )

        # Log load info details
        if load_info.has_failed_jobs:
            logger.error("Generation ingestion had failures")
            for package in load_info.load_packages:
                for job in package.jobs["failed_jobs"]:
                    logger.error(f"Failed job: {job.job_file_path} - {job.failed_message}")
        else:
            logger.info("All generation data loaded successfully")

    except Exception as e:
        logger.error(f"Generation ingestion failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_generation_ingestion()
