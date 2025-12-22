"""Dagster sensors for renewable energy pipeline."""

import sys
from pathlib import Path

from dagster import RunRequest, SensorEvaluationContext, sensor

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from src.config import config

from dags.dagster_project.jobs import daily_ingestion_job


@sensor(
    name="new_data_file_sensor",
    job=daily_ingestion_job,
    description="Trigger ingestion when new Parquet files are detected",
    minimum_interval_seconds=300,  # Check every 5 minutes
)
def new_data_file_sensor(context: SensorEvaluationContext) -> RunRequest | None:
    """
    Sensor that monitors for new Parquet files in raw data directories.

    Triggers the ingestion job when new weather or generation files
    are detected in the configured data directories.

    Parameters
    ----------
    context : SensorEvaluationContext
        Dagster sensor context

    Returns
    -------
    RunRequest | None
        Run request if new files detected, None otherwise

    """
    # Get last processed files from cursor
    cursor_state = context.cursor or "0"
    last_file_count = int(cursor_state)

    # Check for new files
    weather_files = list(config.weather_raw_path.glob("weather_*.parquet"))
    generation_files = list(config.generation_raw_path.glob("generation_*.parquet"))

    current_file_count = len(weather_files) + len(generation_files)

    context.log.info(
        f"Found {len(weather_files)} weather files and "
        f"{len(generation_files)} generation files"
    )

    # Trigger if new files detected
    if current_file_count > last_file_count:
        context.log.info(
            f"New files detected: {current_file_count - last_file_count} files"
        )

        # Update cursor
        context.update_cursor(str(current_file_count))

        return RunRequest(
            run_key=f"new_files_{current_file_count}",
            run_config={},
        )

    context.log.info("No new files detected")
    return None


__all__ = ["new_data_file_sensor"]
