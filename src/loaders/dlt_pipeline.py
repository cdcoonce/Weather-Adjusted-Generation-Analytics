"""Main dlt pipeline orchestrator for renewable energy data ingestion."""

from pathlib import Path

import dlt

from src.config import config
from src.loaders.generation_loader import (
    load_generation_parquet,
    run_generation_ingestion,
)
from src.loaders.weather_loader import (
    load_weather_parquet,
    run_weather_ingestion,
)
from src.utils import get_logger

logger = get_logger(__name__)


def run_full_ingestion(
    weather_files: list[Path] | None = None,
    generation_files: list[Path] | None = None,
) -> None:
    """
    Run complete data ingestion pipeline for weather and generation data.

    Executes both weather and generation ingestion pipelines sequentially,
    loading all data into DuckDB.

    Parameters
    ----------
    weather_files : list[Path], optional
        Specific weather files to ingest. If None, ingests all files.
    generation_files : list[Path], optional
        Specific generation files to ingest. If None, ingests all files.

    Returns
    -------
    None

    Examples
    --------
    >>> # Ingest all available data
    >>> run_full_ingestion()
    >>>
    >>> # Ingest specific files
    >>> weather_files = [Path("data/raw/weather/weather_2023-01-01.parquet")]
    >>> generation_files = [Path("data/raw/generation/generation_2023-01-01.parquet")]
    >>> run_full_ingestion(weather_files, generation_files)

    """
    logger.info("Starting full renewable energy data ingestion")

    try:
        # Ensure directories exist
        config.ensure_directories()

        # Run weather ingestion
        logger.info("=" * 60)
        logger.info("WEATHER DATA INGESTION")
        logger.info("=" * 60)
        run_weather_ingestion(file_paths=weather_files)

        # Run generation ingestion
        logger.info("=" * 60)
        logger.info("GENERATION DATA INGESTION")
        logger.info("=" * 60)
        run_generation_ingestion(file_paths=generation_files)

        logger.info("=" * 60)
        logger.info("FULL INGESTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Full ingestion failed: {e}", exc_info=True)
        raise


def run_combined_pipeline(
    weather_files: list[Path] | None = None,
    generation_files: list[Path] | None = None,
) -> None:
    """
    Run weather and generation ingestion in a single dlt pipeline.

    Combines both resources into one pipeline execution for efficiency.

    Parameters
    ----------
    weather_files : list[Path], optional
        Specific weather files to ingest
    generation_files : list[Path], optional
        Specific generation files to ingest

    Returns
    -------
    None

    """
    logger.info("Starting combined dlt pipeline")

    try:
        # Create combined pipeline
        pipeline = dlt.pipeline(
            pipeline_name=config.dlt_pipeline_name,
            destination=dlt.destinations.duckdb(
                credentials=str(config.duckdb_path),
            ),
            dataset_name=config.dlt_schema,
            progress="log",
        )

        # Run both resources together
        load_info = pipeline.run(
            [
                load_weather_parquet(file_paths=weather_files),
                load_generation_parquet(file_paths=generation_files),
            ],
            loader_file_format="jsonl",
        )

        logger.info(
            f"Combined ingestion completed: {load_info}",
            extra={
                "extra_fields": {
                    "pipeline_name": pipeline.pipeline_name,
                    "destination": str(config.duckdb_path),
                }
            },
        )

        # Log results
        if load_info.has_failed_jobs:
            logger.error("Combined ingestion had failures")
            for package in load_info.load_packages:
                for job in package.jobs["failed_jobs"]:
                    logger.error(f"Failed job: {job.job_file_path} - {job.failed_message}")
        else:
            logger.info("All data loaded successfully")

    except Exception as e:
        logger.error(f"Combined ingestion failed: {e}", exc_info=True)
        raise


def verify_ingestion() -> None:
    """
    Verify that data has been successfully ingested into DuckDB.

    Queries the DuckDB database to check row counts and data quality.

    Returns
    -------
    None

    """
    import duckdb

    logger.info("Verifying data ingestion")

    try:
        con = duckdb.connect(str(config.duckdb_path))

        # List all tables
        tables = con.execute(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{config.dlt_schema}'"
        ).fetchall()

        logger.info(f"Found {len(tables)} tables in schema '{config.dlt_schema}'")

        for (table_name,) in tables:
            # Get row count
            count = con.execute(
                f"SELECT COUNT(*) FROM {config.dlt_schema}.{table_name}"
            ).fetchone()[0]

            logger.info(f"  {table_name}: {count:,} rows")

            # Show sample data
            sample = con.execute(
                f"SELECT * FROM {config.dlt_schema}.{table_name} LIMIT 3"
            ).fetchall()

            print(f"\nSample from {table_name}:")
            if sample:
                # Print column names
                columns = [desc[1] for desc in con.execute(f"PRAGMA table_info({config.dlt_schema}.{table_name})").fetchall()]
                print(f"Columns: {', '.join(columns)}")
                # Print sample rows
                for row in sample:
                    print(row)
            else:
                print("No data found")

        con.close()
        logger.info("Verification completed")

    except Exception as e:
        logger.error(f"Verification failed: {e}", exc_info=True)
        raise


def main() -> None:
    """
    Main entry point for dlt pipeline execution.

    Runs full ingestion and verification.

    Returns
    -------
    None

    """
    logger.info("Starting dlt pipeline main execution")

    # Run full ingestion
    run_full_ingestion()

    # Verify results
    verify_ingestion()

    logger.info("dlt pipeline execution completed successfully")


if __name__ == "__main__":
    main()
