"""Dagster jobs and schedules for the WAGA pipeline.

Historically (macOS launchd era) this module held three independently-timed
UTC schedules that staggered ingestion, dbt, and analytics so each stage
completed before its downstream consumer ran. The pipeline now runs from a
Dagster OSS webserver/daemon on a Linux home server (DockerRunLauncher: each
run is a fresh container from this repo's gRPC code-location image), where
the daemon's own dependency-aware execution replaces manual staggering — so
there are two jobs instead of three schedules-with-cron-offsets:

1. ``waga_daily_job`` — ingestion -> dbt -> dashboard export -> dashboard
   publish, as one job. Dagster resolves the step order from the asset
   graph's dependencies (see the dbt source ``meta.dagster.asset_key``
   lineage added in ``dbt/renewable_dbt/models/staging/*/  _*__sources.yml``)
   rather than three separately-cron'd schedules racing each other.
2. ``waga_weekly_job`` — the weekly correlation analysis.

Both schedules default to :attr:`~dagster.DefaultScheduleStatus.STOPPED` —
see ``docs/local-scheduling.md`` for the cutover plan (start them on the
server once the Docker image is verified; launchd remains a manual
fallback and must not run at the same time as these schedules).
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

# Ingestion (partitioned) + all dbt models (group "default", empty when no
# manifest is present, e.g. in CI) + the two dashboard assets. One job so the
# Dagster daemon's dependency-aware execution — not a cron offset — is what
# keeps dbt from running before ingestion lands.
waga_daily_job = define_asset_job(
    name="waga_daily_job",
    selection=(
        AssetSelection.assets(
            "waga_weather_ingestion",
            "waga_generation_ingestion",
        )
        | AssetSelection.groups("default")
        | AssetSelection.assets("waga_dashboard_export_build")
        | AssetSelection.assets("waga_dashboard_publish")
    ),
    tags={"dagster/max_runtime": "5400"},
)

# Fires daily at the ingestion partitions' own cadence (see
# assets/ingestion/partitions.py: INGESTION_PARTITIONS, timezone
# "America/Phoenix") at 06:00 local. build_schedule_from_partitioned_job
# derives execution_timezone from the job's time-partitioned assets and
# rejects an explicit execution_timezone override for such a job.
waga_daily_job_schedule = build_schedule_from_partitioned_job(
    job=waga_daily_job,
    hour_of_day=6,
    minute_of_hour=0,
    default_status=DefaultScheduleStatus.STOPPED,
)

waga_weekly_job = define_asset_job(
    name="waga_weekly_job",
    selection=AssetSelection.assets("waga_correlation_analysis"),
    tags={"dagster/max_runtime": "1800"},
)

waga_weekly_job_schedule = ScheduleDefinition(
    name="waga_weekly_job_schedule",
    job=waga_weekly_job,
    cron_schedule="30 6 * * 1",
    execution_timezone="America/Phoenix",
    default_status=DefaultScheduleStatus.STOPPED,
)
