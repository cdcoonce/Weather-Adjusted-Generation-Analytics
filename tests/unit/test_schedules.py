"""Unit tests for WAGA pipeline schedules."""

from __future__ import annotations

import pytest

from weather_analytics.schedules import (
    waga_daily_dbt_schedule,
    waga_daily_ingestion_schedule,
    waga_weekly_analytics_schedule,
)


@pytest.mark.unit
class TestScheduleDefinitions:
    """Verify schedule configuration is correct."""

    def test_ingestion_schedule_name(self) -> None:
        assert waga_daily_ingestion_schedule.name == "waga_daily_ingestion"

    def test_ingestion_schedule_cron(self) -> None:
        assert waga_daily_ingestion_schedule.cron_schedule == "0 6 * * *"

    def test_ingestion_schedule_timezone(self) -> None:
        assert waga_daily_ingestion_schedule.execution_timezone == "UTC"

    def test_dbt_schedule_name(self) -> None:
        assert waga_daily_dbt_schedule.name == "waga_daily_dbt"

    def test_dbt_schedule_cron(self) -> None:
        assert waga_daily_dbt_schedule.cron_schedule == "0 7 * * *"

    def test_dbt_schedule_timezone(self) -> None:
        assert waga_daily_dbt_schedule.execution_timezone == "UTC"

    def test_analytics_schedule_name(self) -> None:
        assert waga_weekly_analytics_schedule.name == "waga_weekly_analytics"

    def test_analytics_schedule_cron(self) -> None:
        assert waga_weekly_analytics_schedule.cron_schedule == "0 8 * * 1"

    def test_analytics_schedule_timezone(self) -> None:
        assert waga_weekly_analytics_schedule.execution_timezone == "UTC"
