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

Both schedules default to :attr:`~dagster.DefaultScheduleStatus.STOPPED`.
The cutover happened on 2026-09-18: both were started on the server and the
Mac launchd jobs were unloaded and their plists renamed ``.plist.disabled``.
The default is left at STOPPED deliberately — Dagster only consults it when
no instigator state is stored, so a schedule started by hand survives
redeploys, and a STOPPED default means a brand-new instance comes up idle
rather than ingesting and publishing before anyone has checked it. launchd
remains a manual fallback and must never run at the same time as these
schedules; two runners would ingest and publish the same dashboard.

Their times are staggered off the host's other tenant — see the comments on
each schedule below.
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
# "America/Phoenix") at 06:15 local. build_schedule_from_partitioned_job
# derives execution_timezone from the job's time-partitioned assets and
# rejects an explicit execution_timezone override for such a job.
#
# 06:15, not 06:00, to stay off the rammingspeed host's other tenant. The
# oura-pipeline code location runs `daily_oura_job` at 06:00 America/Phoenix
# and the instance allows ONE concurrent run (dagster.yaml:
# QueuedRunCoordinator max_concurrent_runs: 1, two CPU cores), so an
# identical 06:00 tick put both run requests in the queue at the same instant
# and one sat PIPELINE_ENQUEUED behind the other. Nothing failed, but a tick
# could read SUCCESS while its run had not started, which makes "did the
# dashboard refresh?" ambiguous for minutes after 06:00. Measured on
# 2026-09-18: daily_oura_job max 209 s, this job max 168 s. A 15-minute
# offset is ~4x oura's worst case and still clears the 06:30/06:45 weeklies.
waga_daily_job_schedule = build_schedule_from_partitioned_job(
    job=waga_daily_job,
    hour_of_day=6,
    minute_of_hour=15,
    default_status=DefaultScheduleStatus.STOPPED,
)

waga_weekly_job = define_asset_job(
    name="waga_weekly_job",
    selection=AssetSelection.assets("waga_correlation_analysis"),
    tags={"dagster/max_runtime": "1800"},
)

# 06:45 Monday for the same reason the daily moved off 06:00: oura's
# weekly_report_job_schedule is "30 6 * * 1" America/Phoenix, so an identical
# 06:30 put both weeklies in the single run slot at once. oura's weekly
# succeeds-as-skip below two days of data and reports its send through Python
# logging (which never reaches Dagster compute logs), so it has to be verified
# against SES send statistics -- leaving it unqueued keeps that verification
# unambiguous.
waga_weekly_job_schedule = ScheduleDefinition(
    name="waga_weekly_job_schedule",
    job=waga_weekly_job,
    cron_schedule="45 6 * * 1",
    execution_timezone="America/Phoenix",
    default_status=DefaultScheduleStatus.STOPPED,
)
