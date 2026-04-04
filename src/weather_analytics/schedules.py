"""Dagster schedules for the WAGA pipeline.

Three schedules stagger execution so each pipeline stage completes
before its downstream consumers are triggered:

1. **Ingestion** — daily at 06:00 UTC
2. **dbt transforms** — daily at 07:00 UTC (after ingestion)
3. **Analytics** — weekly Monday at 08:00 UTC
"""

from dagster import AssetSelection, ScheduleDefinition

waga_daily_ingestion_schedule = ScheduleDefinition(
    name="waga_daily_ingestion",
    target=AssetSelection.assets("waga_weather_ingestion", "waga_generation_ingestion"),
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
)

waga_daily_dbt_schedule = ScheduleDefinition(
    name="waga_daily_dbt",
    target=AssetSelection.assets("waga_dbt_assets"),
    cron_schedule="0 7 * * *",
    execution_timezone="UTC",
)

waga_weekly_analytics_schedule = ScheduleDefinition(
    name="waga_weekly_analytics",
    target=AssetSelection.assets("waga_correlation_analysis"),
    cron_schedule="0 8 * * 1",
    execution_timezone="UTC",
)
