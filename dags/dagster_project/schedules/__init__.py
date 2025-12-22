"""Dagster schedules for renewable energy pipeline."""

from dagster import ScheduleDefinition

from dags.dagster_project.jobs import (
    correlation_job,
    daily_dbt_job,
    daily_ingestion_job,
)

# Daily ingestion at 6 AM
daily_ingestion_schedule = ScheduleDefinition(
    name="daily_ingestion_schedule",
    job=daily_ingestion_job,
    cron_schedule="0 6 * * *",  # 6 AM daily
    description="Run daily data ingestion at 6 AM",
)

# Weekly performance summary on Monday at 8 AM
weekly_performance_schedule = ScheduleDefinition(
    name="weekly_performance_schedule",
    job=correlation_job,
    cron_schedule="0 8 * * 1",  # 8 AM every Monday
    description="Generate weekly performance summary on Mondays",
)

# Daily dbt run after ingestion (7 AM)
daily_dbt_schedule = ScheduleDefinition(
    name="daily_dbt_schedule",
    job=daily_dbt_job,
    cron_schedule="0 7 * * *",  # 7 AM daily
    description="Run dbt transformations daily at 7 AM",
)

__all__ = [
    "daily_ingestion_schedule",
    "weekly_performance_schedule",
    "daily_dbt_schedule",
]
