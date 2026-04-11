"""Dagster schedules for the WAGA pipeline.

Three schedules stagger execution so each pipeline stage completes
before its downstream consumers are triggered:

1. **Ingestion** — daily at 06:00 UTC (yesterday's partition)
2. **dbt transforms** — daily at 07:00 UTC (after ingestion)
3. **Analytics** — weekly Monday at 08:00 UTC
"""

from datetime import timedelta

from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    schedule,
)


@schedule(
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    name="waga_daily_ingestion",
    target=AssetSelection.assets(
        "waga_weather_ingestion",
        "waga_generation_ingestion",
    ),
)
def waga_daily_ingestion_schedule(
    context: ScheduleEvaluationContext,
) -> RunRequest:
    """Materialize yesterday's partition for both ingestion assets.

    Parameters
    ----------
    context : ScheduleEvaluationContext
        Schedule context providing the scheduled execution time.

    Returns
    -------
    RunRequest
        Run request targeting yesterday's partition.
    """
    scheduled_date = context.scheduled_execution_time
    if scheduled_date is None:
        msg = "Schedule requires a scheduled_execution_time"
        raise RuntimeError(msg)
    yesterday = (scheduled_date - timedelta(days=1)).strftime("%Y-%m-%d")

    return RunRequest(
        run_key=f"ingestion_{yesterday}",
        partition_key=yesterday,
    )


waga_daily_dbt_schedule = ScheduleDefinition(
    name="waga_daily_dbt",
    target=AssetSelection.groups("default"),
    cron_schedule="0 7 * * *",
    execution_timezone="UTC",
)

waga_weekly_analytics_schedule = ScheduleDefinition(
    name="waga_weekly_analytics",
    target=AssetSelection.assets("waga_correlation_analysis"),
    cron_schedule="0 8 * * 1",
    execution_timezone="UTC",
)
