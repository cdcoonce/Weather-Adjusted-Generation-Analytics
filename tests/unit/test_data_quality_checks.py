"""Unit tests for WAGA data quality asset checks."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from weather_analytics.checks.data_quality import (
    FRESHNESS_THRESHOLD_HOURS,
    MIN_ROW_COUNT,
    waga_generation_freshness_check,
    waga_generation_value_range_check,
    waga_mart_correlation_row_count_check,
    waga_mart_performance_row_count_check,
    waga_raw_generation_row_count_check,
    waga_raw_weather_row_count_check,
    waga_weather_freshness_check,
    waga_weather_value_range_check,
)


def _make_mock_snowflake(fetchone_return: tuple) -> MagicMock:
    """Build a mock WAGASnowflakeResource with a preset fetchone value."""
    mock_resource = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_resource.get_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = fetchone_return
    return mock_resource


# ===================================================================
# Freshness checks
# ===================================================================


class TestWeatherFreshnessCheck:
    """Tests for waga_weather_freshness_check."""

    @pytest.mark.unit
    def test_passes_for_recent_data(self) -> None:
        recent_ts = datetime.now(tz=UTC) - timedelta(hours=1)
        mock_sf = _make_mock_snowflake((recent_ts,))
        result = waga_weather_freshness_check(snowflake=mock_sf)
        assert result.passed is True
        assert result.metadata is not None
        assert "max_timestamp" in result.metadata

    @pytest.mark.unit
    def test_fails_for_stale_data(self) -> None:
        stale_ts = datetime.now(tz=UTC) - timedelta(hours=FRESHNESS_THRESHOLD_HOURS + 1)
        mock_sf = _make_mock_snowflake((stale_ts,))
        result = waga_weather_freshness_check(snowflake=mock_sf)
        assert result.passed is False

    @pytest.mark.unit
    def test_fails_for_no_rows(self) -> None:
        mock_sf = _make_mock_snowflake((None,))
        result = waga_weather_freshness_check(snowflake=mock_sf)
        assert result.passed is False
        assert result.metadata is not None
        assert "reason" in result.metadata

    @pytest.mark.unit
    def test_handles_naive_timestamp(self) -> None:
        """Naive timestamps should be treated as UTC."""
        recent_naive = datetime.now(tz=UTC).replace(tzinfo=None) - timedelta(hours=1)
        # The check adds UTC tzinfo to naive timestamps
        mock_sf = _make_mock_snowflake((recent_naive,))
        result = waga_weather_freshness_check(snowflake=mock_sf)
        assert result.passed is True

    @pytest.mark.unit
    def test_closes_connection(self) -> None:
        recent_ts = datetime.now(tz=UTC)
        mock_sf = _make_mock_snowflake((recent_ts,))
        waga_weather_freshness_check(snowflake=mock_sf)
        mock_sf.get_connection.return_value.close.assert_called_once()


class TestGenerationFreshnessCheck:
    """Tests for waga_generation_freshness_check."""

    @pytest.mark.unit
    def test_passes_for_recent_data(self) -> None:
        recent_ts = datetime.now(tz=UTC) - timedelta(hours=1)
        mock_sf = _make_mock_snowflake((recent_ts,))
        result = waga_generation_freshness_check(snowflake=mock_sf)
        assert result.passed is True

    @pytest.mark.unit
    def test_fails_for_stale_data(self) -> None:
        stale_ts = datetime.now(tz=UTC) - timedelta(hours=FRESHNESS_THRESHOLD_HOURS + 1)
        mock_sf = _make_mock_snowflake((stale_ts,))
        result = waga_generation_freshness_check(snowflake=mock_sf)
        assert result.passed is False


# ===================================================================
# Row count checks
# ===================================================================


class TestRowCountChecks:
    """Tests for all row count checks."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "check_fn",
        [
            waga_raw_weather_row_count_check,
            waga_raw_generation_row_count_check,
            waga_mart_performance_row_count_check,
            waga_mart_correlation_row_count_check,
        ],
        ids=[
            "raw_weather",
            "raw_generation",
            "mart_performance",
            "mart_correlation",
        ],
    )
    def test_passes_for_sufficient_rows(self, check_fn) -> None:  # type: ignore[no-untyped-def]
        mock_sf = _make_mock_snowflake((MIN_ROW_COUNT,))
        result = check_fn(snowflake=mock_sf)
        assert result.passed is True
        assert result.metadata["row_count"].value == MIN_ROW_COUNT

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "check_fn",
        [
            waga_raw_weather_row_count_check,
            waga_raw_generation_row_count_check,
            waga_mart_performance_row_count_check,
            waga_mart_correlation_row_count_check,
        ],
        ids=[
            "raw_weather",
            "raw_generation",
            "mart_performance",
            "mart_correlation",
        ],
    )
    def test_fails_for_too_few_rows(self, check_fn) -> None:  # type: ignore[no-untyped-def]
        mock_sf = _make_mock_snowflake((MIN_ROW_COUNT - 1,))
        result = check_fn(snowflake=mock_sf)
        assert result.passed is False

    @pytest.mark.unit
    def test_row_count_metadata_contains_minimum(self) -> None:
        mock_sf = _make_mock_snowflake((500,))
        result = waga_raw_weather_row_count_check(snowflake=mock_sf)
        assert result.metadata["minimum"].value == MIN_ROW_COUNT

    @pytest.mark.unit
    def test_closes_connection(self) -> None:
        mock_sf = _make_mock_snowflake((200,))
        waga_raw_weather_row_count_check(snowflake=mock_sf)
        mock_sf.get_connection.return_value.close.assert_called_once()


# ===================================================================
# Value range checks
# ===================================================================


class TestWeatherValueRangeCheck:
    """Tests for waga_weather_value_range_check."""

    @pytest.mark.unit
    def test_passes_when_no_violations(self) -> None:
        mock_sf = _make_mock_snowflake((0,))
        result = waga_weather_value_range_check(snowflake=mock_sf)
        assert result.passed is True
        assert result.metadata["out_of_range_rows"].value == 0

    @pytest.mark.unit
    def test_fails_when_violations_exist(self) -> None:
        mock_sf = _make_mock_snowflake((42,))
        result = waga_weather_value_range_check(snowflake=mock_sf)
        assert result.passed is False
        assert result.metadata["out_of_range_rows"].value == 42

    @pytest.mark.unit
    def test_closes_connection(self) -> None:
        mock_sf = _make_mock_snowflake((0,))
        waga_weather_value_range_check(snowflake=mock_sf)
        mock_sf.get_connection.return_value.close.assert_called_once()


class TestGenerationValueRangeCheck:
    """Tests for waga_generation_value_range_check."""

    @pytest.mark.unit
    def test_passes_when_no_violations(self) -> None:
        mock_sf = _make_mock_snowflake((0,))
        result = waga_generation_value_range_check(snowflake=mock_sf)
        assert result.passed is True

    @pytest.mark.unit
    def test_fails_when_violations_exist(self) -> None:
        mock_sf = _make_mock_snowflake((10,))
        result = waga_generation_value_range_check(snowflake=mock_sf)
        assert result.passed is False
