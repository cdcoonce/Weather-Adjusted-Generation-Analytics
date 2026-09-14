"""Unit tests for WAGA pipeline jobs and schedules.

Pins the home-server cutover: one daily job (ingestion -> dbt -> dashboard
export -> dashboard publish) driven by the ingestion partitions' own
schedule, plus a weekly analytics job — both America/Phoenix, both
default-STOPPED until started at cutover (see docs/local-scheduling.md).
"""

from __future__ import annotations

import pytest
from dagster import DefaultScheduleStatus

import weather_analytics.schedules as schedules_module
from weather_analytics.assets.ingestion.partitions import INGESTION_PARTITIONS
from weather_analytics.definitions import defs
from weather_analytics.schedules import (
    waga_daily_job,
    waga_daily_job_schedule,
    waga_weekly_job,
    waga_weekly_job_schedule,
)


@pytest.mark.unit
class TestIngestionPartitions:
    """The shared partitions def both ingestion assets use."""

    def test_timezone_is_phoenix(self) -> None:
        assert INGESTION_PARTITIONS.timezone == "America/Phoenix"

    def test_start_date_unchanged(self) -> None:
        assert INGESTION_PARTITIONS.start.strftime("%Y-%m-%d") == "2023-01-01"


@pytest.mark.unit
class TestDailyJobSchedule:
    """build_schedule_from_partitioned_job(waga_daily_job, hour_of_day=6, ...)."""

    def test_name(self) -> None:
        assert waga_daily_job.name == "waga_daily_job"

    def test_max_runtime_tag(self) -> None:
        assert waga_daily_job.tags.get("dagster/max_runtime") == "5400"

    def test_schedule_hour_and_minute(self) -> None:
        # This is the unresolved schedule (build_schedule_from_partitioned_job
        # defers cron/timezone resolution to the job's partitioned assets) —
        # see TestResolvedDefinitions for the resolved cron/timezone.
        assert waga_daily_job_schedule.hour_of_day == 6
        assert waga_daily_job_schedule.minute_of_hour == 0

    def test_schedule_default_status_stopped(self) -> None:
        assert waga_daily_job_schedule.default_status == DefaultScheduleStatus.STOPPED


@pytest.mark.unit
class TestWeeklyJobSchedule:
    """ScheduleDefinition(waga_weekly_job, cron_schedule="30 6 * * 1", ...)."""

    def test_name(self) -> None:
        assert waga_weekly_job.name == "waga_weekly_job"

    def test_max_runtime_tag(self) -> None:
        assert waga_weekly_job.tags.get("dagster/max_runtime") == "1800"

    def test_schedule_name(self) -> None:
        assert waga_weekly_job_schedule.name == "waga_weekly_job_schedule"

    def test_schedule_cron(self) -> None:
        assert waga_weekly_job_schedule.cron_schedule == "30 6 * * 1"

    def test_schedule_timezone(self) -> None:
        assert waga_weekly_job_schedule.execution_timezone == "America/Phoenix"

    def test_schedule_default_status_stopped(self) -> None:
        assert waga_weekly_job_schedule.default_status == DefaultScheduleStatus.STOPPED

    def test_schedule_target(self) -> None:
        assert waga_weekly_job_schedule.job_name == "waga_weekly_job"


@pytest.mark.unit
class TestResolvedDefinitions:
    """Schedule properties as resolved through the Definitions object — the
    execution_timezone for a build_schedule_from_partitioned_job schedule is
    only set once Dagster resolves it against the job's partitioned assets."""

    def test_daily_schedule_resolves_to_phoenix_timezone_and_cron(self) -> None:
        repo = defs.get_repository_def()
        resolved = repo.schedule_defs
        daily = next(s for s in resolved if s.name == "waga_daily_job_schedule")
        assert daily.execution_timezone == "America/Phoenix"
        assert daily.cron_schedule == "0 6 * * *"

    def test_daily_job_resolves_without_manifest(self) -> None:
        """CI has no dbt manifest.json — group:default is then empty, and the
        job must still resolve (mixed partitioned + unpartitioned assets in
        one job selection is accepted either way)."""
        assert defs.resolve_job_def("waga_daily_job") is not None

    def test_weekly_job_resolves(self) -> None:
        assert defs.resolve_job_def("waga_weekly_job") is not None


@pytest.mark.unit
class TestOldSchedulesRemoved:
    """The three UTC-cron schedules from the launchd era are gone."""

    def test_old_names_not_importable(self) -> None:
        for old_name in (
            "waga_daily_ingestion_schedule",
            "waga_daily_dbt_schedule",
            "waga_weekly_analytics_schedule",
            "waga_daily_ingestion",
            "waga_daily_dbt",
            "waga_weekly_analytics",
        ):
            assert not hasattr(schedules_module, old_name)
