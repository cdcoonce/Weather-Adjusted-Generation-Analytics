"""Unit tests for ``weather_analytics.dashboard.components.asset_view``.

Tests exercise the pure data-preparation functions extracted from asset_view:
- ``_filter_asset_daily`` — filter daily_df to a single asset + date range
- ``_filter_asset_weather`` — filter weather_df to a single asset + date range
- ``_get_asset_type`` — return inferred_asset_type for an asset_id
- ``_prep_expected_vs_actual`` — extract dates, actual, expected MWh series
- ``_prep_rolling_cf`` — extract dates + three CF series
- ``_prep_scatter`` — extract (x_vals, y_vals, r_squared)
- ``_prep_stacked_hours`` — return daily_df subset with hour columns
- ``_fit_regression`` — linear regression line helper

``asset_panel`` is smoke-tested only — Panel rendering is not exercised.
"""

from __future__ import annotations

from datetime import datetime

import panel as pn
import polars as pl
import pytest

from weather_analytics.dashboard.components.asset_view import (
    _filter_asset_daily,
    _filter_asset_weather,
    _fit_regression,
    _get_asset_type,
    _prep_expected_vs_actual,
    _prep_rolling_cf,
    _prep_scatter,
    _prep_stacked_hours,
    asset_panel,
)
from weather_analytics.dashboard.components.filters import Filters

# ---------------------------------------------------------------------------
# Row counts and tolerances derived from the fixture data below.
# ---------------------------------------------------------------------------
_WIND001_DAILY_ROWS = 3
_SOLAR001_DAILY_ROWS = 2
_WIND001_WEATHER_ROWS = 3
_TWO_ROWS = 2
_ONE_ROW = 1
_REGRESSION_TOLERANCE = 1e-9

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_DAILY_DF = pl.DataFrame(
    {
        "asset_id": ["WIND_001", "WIND_001", "WIND_001", "SOLAR_001", "SOLAR_001"],
        "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-01", "2025-01-02"],
        "total_net_generation_mwh": [100.0, 200.0, 150.0, 50.0, 90.0],
        "daily_capacity_factor": [0.40, 0.50, 0.45, 0.30, 0.38],
        "avg_availability_pct": [98.0, 99.0, 97.5, 96.0, 97.5],
        "avg_wind_speed_mps": [8.0, 10.0, 9.0, 0.0, 0.0],
        "avg_ghi": [0.0, 0.0, 0.0, 250.0, 300.0],
        "avg_temperature_c": [15.0, 16.0, 14.0, 20.0, 22.0],
        "excellent_hours": [4.0, 6.0, 5.0, 2.0, 3.0],
        "good_hours": [8.0, 10.0, 9.0, 6.0, 7.0],
        "fair_hours": [6.0, 4.0, 5.0, 8.0, 7.0],
        "poor_hours": [6.0, 4.0, 5.0, 8.0, 7.0],
        "total_curtailment_mwh": [5.0, 3.0, 4.0, 1.0, 2.0],
        "daily_performance_rating": [0.85, 0.90, 0.87, 0.80, 0.82],
        "data_completeness_pct": [100.0, 100.0, 100.0, 100.0, 100.0],
    }
)

_WEATHER_DF = pl.DataFrame(
    {
        "asset_id": ["WIND_001", "WIND_001", "WIND_001", "SOLAR_001", "SOLAR_001"],
        "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-01", "2025-01-02"],
        "performance_score": [0.80, 0.90, 0.85, 0.70, 0.78],
        "performance_category": ["Good", "Excellent", "Good", "Fair", "Good"],
        "avg_expected_generation_mwh": [120.0, 180.0, 160.0, 60.0, 95.0],
        "avg_actual_generation_mwh": [100.0, 200.0, 150.0, 50.0, 90.0],
        "avg_performance_ratio_pct": [83.3, 111.1, 93.8, 83.3, 94.7],
        "wind_r_squared": [0.85, 0.85, 0.85, 0.0, 0.0],
        "solar_r_squared": [0.0, 0.0, 0.0, 0.72, 0.72],
        "inferred_asset_type": ["Wind", "Wind", "Wind", "Solar", "Solar"],
        "rolling_7d_avg_cf": [0.42, 0.45, 0.44, 0.32, 0.35],
        "rolling_30d_avg_cf": [0.40, 0.41, 0.42, 0.31, 0.33],
    }
)

_EMPTY_DAILY = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Utf8),
        "total_net_generation_mwh": pl.Series([], dtype=pl.Float64),
        "daily_capacity_factor": pl.Series([], dtype=pl.Float64),
        "avg_availability_pct": pl.Series([], dtype=pl.Float64),
        "avg_wind_speed_mps": pl.Series([], dtype=pl.Float64),
        "avg_ghi": pl.Series([], dtype=pl.Float64),
        "avg_temperature_c": pl.Series([], dtype=pl.Float64),
        "excellent_hours": pl.Series([], dtype=pl.Float64),
        "good_hours": pl.Series([], dtype=pl.Float64),
        "fair_hours": pl.Series([], dtype=pl.Float64),
        "poor_hours": pl.Series([], dtype=pl.Float64),
        "total_curtailment_mwh": pl.Series([], dtype=pl.Float64),
        "daily_performance_rating": pl.Series([], dtype=pl.Float64),
        "data_completeness_pct": pl.Series([], dtype=pl.Float64),
    }
)

_EMPTY_WEATHER = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Utf8),
        "performance_score": pl.Series([], dtype=pl.Float64),
        "performance_category": pl.Series([], dtype=pl.Utf8),
        "avg_expected_generation_mwh": pl.Series([], dtype=pl.Float64),
        "avg_actual_generation_mwh": pl.Series([], dtype=pl.Float64),
        "avg_performance_ratio_pct": pl.Series([], dtype=pl.Float64),
        "wind_r_squared": pl.Series([], dtype=pl.Float64),
        "solar_r_squared": pl.Series([], dtype=pl.Float64),
        "inferred_asset_type": pl.Series([], dtype=pl.Utf8),
        "rolling_7d_avg_cf": pl.Series([], dtype=pl.Float64),
        "rolling_30d_avg_cf": pl.Series([], dtype=pl.Float64),
    }
)


# ===========================================================================
# _filter_asset_daily
# ===========================================================================


@pytest.mark.unit
def test_filter_asset_daily_returns_only_matching_asset() -> None:
    """Filter by asset_id returns only that asset's rows."""
    result = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    assert set(result["asset_id"].to_list()) == {"WIND_001"}
    assert result.shape[0] == _WIND001_DAILY_ROWS


@pytest.mark.unit
def test_filter_asset_daily_applies_date_start() -> None:
    """date_start removes rows before the given date."""
    result = _filter_asset_daily(_DAILY_DF, "WIND_001", "2025-01-02", "")
    assert result.shape[0] == _TWO_ROWS
    assert all(d >= "2025-01-02" for d in result["date"].to_list())


@pytest.mark.unit
def test_filter_asset_daily_applies_date_end() -> None:
    """date_end removes rows after the given date."""
    result = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "2025-01-02")
    assert result.shape[0] == _TWO_ROWS
    assert all(d <= "2025-01-02" for d in result["date"].to_list())


@pytest.mark.unit
def test_filter_asset_daily_date_range() -> None:
    """Combined date range returns only rows within [start, end]."""
    result = _filter_asset_daily(_DAILY_DF, "WIND_001", "2025-01-02", "2025-01-02")
    assert result.shape[0] == _ONE_ROW
    assert result["date"].to_list() == ["2025-01-02"]


@pytest.mark.unit
def test_filter_asset_daily_empty_df_returns_empty() -> None:
    """Empty input DataFrame returns empty DataFrame."""
    result = _filter_asset_daily(_EMPTY_DAILY, "WIND_001", "", "")
    assert result.shape[0] == 0


# ===========================================================================
# _filter_asset_weather
# ===========================================================================


@pytest.mark.unit
def test_filter_asset_weather_returns_only_matching_asset() -> None:
    """Filter by asset_id returns only that asset's rows."""
    result = _filter_asset_weather(_WEATHER_DF, "WIND_001", "", "")
    assert set(result["asset_id"].to_list()) == {"WIND_001"}
    assert result.shape[0] == _WIND001_WEATHER_ROWS


@pytest.mark.unit
def test_filter_asset_weather_applies_date_range() -> None:
    """Date range filter applies correctly."""
    result = _filter_asset_weather(_WEATHER_DF, "WIND_001", "2025-01-02", "2025-01-02")
    assert result.shape[0] == _ONE_ROW
    assert result["date"].to_list() == ["2025-01-02"]


@pytest.mark.unit
def test_filter_asset_weather_empty_df_returns_empty() -> None:
    """Empty input DataFrame returns empty DataFrame."""
    result = _filter_asset_weather(_EMPTY_WEATHER, "WIND_001", "", "")
    assert result.shape[0] == 0


# ===========================================================================
# _get_asset_type
# ===========================================================================


@pytest.mark.unit
def test_get_asset_type_wind() -> None:
    """Returns 'Wind' for a Wind asset."""
    filtered = _filter_asset_weather(_WEATHER_DF, "WIND_001", "", "")
    assert _get_asset_type(filtered) == "Wind"


@pytest.mark.unit
def test_get_asset_type_solar() -> None:
    """Returns 'Solar' for a Solar asset."""
    filtered = _filter_asset_weather(_WEATHER_DF, "SOLAR_001", "", "")
    assert _get_asset_type(filtered) == "Solar"


@pytest.mark.unit
def test_get_asset_type_empty_returns_empty_string() -> None:
    """Returns empty string when DataFrame is empty."""
    assert _get_asset_type(_EMPTY_WEATHER) == ""


# ===========================================================================
# _prep_expected_vs_actual
# ===========================================================================


@pytest.mark.unit
def test_prep_expected_vs_actual_returns_three_lists() -> None:
    """Returns (dates, actual_vals, expected_vals) triple."""
    filtered = _filter_asset_weather(_WEATHER_DF, "WIND_001", "", "")
    dates, actual, expected = _prep_expected_vs_actual(filtered)
    assert len(dates) == _WIND001_WEATHER_ROWS
    assert len(actual) == _WIND001_WEATHER_ROWS
    assert len(expected) == _WIND001_WEATHER_ROWS


@pytest.mark.unit
def test_prep_expected_vs_actual_dates_are_datetime() -> None:
    """Date values are datetime objects (required by Bokeh datetime axis)."""
    filtered = _filter_asset_weather(_WEATHER_DF, "WIND_001", "", "")
    dates, _actual, _expected = _prep_expected_vs_actual(filtered)
    for d in dates:
        assert isinstance(d, datetime)


@pytest.mark.unit
def test_prep_expected_vs_actual_values_match_fixture() -> None:
    """Actual and expected MWh lists match fixture data (sorted by date)."""
    filtered = _filter_asset_weather(_WEATHER_DF, "WIND_001", "", "")
    _dates, actual, expected = _prep_expected_vs_actual(filtered)
    assert actual == [100.0, 200.0, 150.0]
    assert expected == [120.0, 180.0, 160.0]


@pytest.mark.unit
def test_prep_expected_vs_actual_empty_returns_empty_lists() -> None:
    """Empty DataFrame returns three empty lists."""
    dates, actual, expected = _prep_expected_vs_actual(_EMPTY_WEATHER)
    assert dates == []
    assert actual == []
    assert expected == []


# ===========================================================================
# _prep_rolling_cf
# ===========================================================================


@pytest.mark.unit
def test_prep_rolling_cf_returns_four_lists() -> None:
    """Returns (dates, cf_7d, cf_30d, raw_cf) four-tuple."""
    w_filtered = _filter_asset_weather(_WEATHER_DF, "WIND_001", "", "")
    d_filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    dates, cf7, cf30, raw = _prep_rolling_cf(w_filtered, d_filtered)
    assert len(dates) == _WIND001_WEATHER_ROWS
    assert len(cf7) == _WIND001_WEATHER_ROWS
    assert len(cf30) == _WIND001_WEATHER_ROWS
    assert len(raw) == _WIND001_WEATHER_ROWS


@pytest.mark.unit
def test_prep_rolling_cf_dates_are_datetime() -> None:
    """Dates are datetime objects."""
    w_filtered = _filter_asset_weather(_WEATHER_DF, "WIND_001", "", "")
    d_filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    dates, _cf7, _cf30, _raw = _prep_rolling_cf(w_filtered, d_filtered)
    for d in dates:
        assert isinstance(d, datetime)


@pytest.mark.unit
def test_prep_rolling_cf_values_match_fixture() -> None:
    """7d and 30d CF values match fixture data (sorted by date)."""
    w_filtered = _filter_asset_weather(_WEATHER_DF, "WIND_001", "", "")
    d_filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    _dates, cf7, cf30, raw = _prep_rolling_cf(w_filtered, d_filtered)
    assert cf7 == [0.42, 0.45, 0.44]
    assert cf30 == [0.40, 0.41, 0.42]
    assert raw == [0.40, 0.50, 0.45]


@pytest.mark.unit
def test_prep_rolling_cf_empty_weather_returns_empty() -> None:
    """Empty weather DataFrame returns four empty lists."""
    dates, cf7, cf30, raw = _prep_rolling_cf(_EMPTY_WEATHER, _EMPTY_DAILY)
    assert dates == []
    assert cf7 == []
    assert cf30 == []
    assert raw == []


# ===========================================================================
# _prep_scatter
# ===========================================================================


@pytest.mark.unit
def test_prep_scatter_wind_uses_wind_speed() -> None:
    """For Wind asset type, x values are avg_wind_speed_mps."""
    filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    x_vals, _y_vals, _r2 = _prep_scatter(filtered, "Wind")
    assert x_vals == [8.0, 10.0, 9.0]


@pytest.mark.unit
def test_prep_scatter_solar_uses_ghi() -> None:
    """For Solar asset type, x values are avg_ghi."""
    filtered = _filter_asset_daily(_DAILY_DF, "SOLAR_001", "", "")
    x_vals, _y_vals, _r2 = _prep_scatter(filtered, "Solar")
    assert x_vals == [250.0, 300.0]


@pytest.mark.unit
def test_prep_scatter_y_vals_are_generation() -> None:
    """Y values are total_net_generation_mwh."""
    filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    _x_vals, y_vals, _r2 = _prep_scatter(filtered, "Wind")
    assert y_vals == [100.0, 200.0, 150.0]


@pytest.mark.unit
def test_prep_scatter_returns_r_squared() -> None:
    """r_squared is a float between 0 and 1 (or exactly 0 for degenerate data)."""
    filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    _x_vals, _y_vals, r2 = _prep_scatter(filtered, "Wind")
    assert isinstance(r2, float)
    assert 0.0 <= r2 <= 1.0


@pytest.mark.unit
def test_prep_scatter_empty_df_returns_empty() -> None:
    """Empty DataFrame returns three empty / zero values."""
    x_vals, y_vals, r2 = _prep_scatter(_EMPTY_DAILY, "Wind")
    assert x_vals == []
    assert y_vals == []
    assert r2 == 0.0


# ===========================================================================
# _prep_stacked_hours
# ===========================================================================


@pytest.mark.unit
def test_prep_stacked_hours_has_hour_columns() -> None:
    """Result DataFrame contains all four hour-category columns."""
    filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    result = _prep_stacked_hours(filtered)
    for col in ("excellent_hours", "good_hours", "fair_hours", "poor_hours"):
        assert col in result.columns


@pytest.mark.unit
def test_prep_stacked_hours_has_date_column() -> None:
    """Result DataFrame retains the date column."""
    filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    result = _prep_stacked_hours(filtered)
    assert "date" in result.columns


@pytest.mark.unit
def test_prep_stacked_hours_row_count_matches_input() -> None:
    """Row count matches the filtered input."""
    filtered = _filter_asset_daily(_DAILY_DF, "WIND_001", "", "")
    result = _prep_stacked_hours(filtered)
    assert result.shape[0] == _WIND001_DAILY_ROWS


@pytest.mark.unit
def test_prep_stacked_hours_empty_returns_empty() -> None:
    """Empty input returns empty DataFrame with expected columns."""
    result = _prep_stacked_hours(_EMPTY_DAILY)
    assert result.shape[0] == 0
    for col in ("excellent_hours", "good_hours", "fair_hours", "poor_hours", "date"):
        assert col in result.columns


# ===========================================================================
# _fit_regression
# ===========================================================================


@pytest.mark.unit
def test_fit_regression_returns_two_lists() -> None:
    """Returns (x_line, y_line) tuple of equal length."""
    x = [1.0, 2.0, 3.0, 4.0]
    y = [2.0, 4.0, 6.0, 8.0]
    x_line, y_line = _fit_regression(x, y)
    assert len(x_line) == len(y_line)


@pytest.mark.unit
def test_fit_regression_x_line_is_sorted() -> None:
    """Returned x_line is sorted ascending."""
    x = [4.0, 1.0, 3.0, 2.0]
    y = [8.0, 2.0, 6.0, 4.0]
    x_line, _y_line = _fit_regression(x, y)
    assert x_line == sorted(x_line)


@pytest.mark.unit
def test_fit_regression_fewer_than_two_points_returns_inputs() -> None:
    """With fewer than 2 points, returns the original x and y unchanged."""
    x_line, y_line = _fit_regression([5.0], [10.0])
    assert x_line == [5.0]
    assert y_line == [10.0]


@pytest.mark.unit
def test_fit_regression_empty_returns_empty() -> None:
    """Empty inputs return empty lists."""
    x_line, y_line = _fit_regression([], [])
    assert x_line == []
    assert y_line == []


@pytest.mark.unit
def test_fit_regression_perfect_linear_relationship() -> None:
    """For y = 2x + 1, the regression line should be y = 2x + 1."""
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [3.0, 5.0, 7.0, 9.0, 11.0]
    x_line, y_line = _fit_regression(x, y)
    for xi, yi in zip(x_line, y_line, strict=False):
        assert abs(yi - (2 * xi + 1)) < _REGRESSION_TOLERANCE


# ===========================================================================
# asset_panel smoke test
# ===========================================================================


@pytest.mark.unit
def test_asset_panel_is_importable_and_callable() -> None:
    """asset_panel must be importable and callable without error."""
    assert callable(asset_panel)


@pytest.mark.unit
def test_asset_panel_returns_panel_column() -> None:
    """asset_panel(filters) returns a pn.Column."""
    f = Filters()
    f._daily_df = _DAILY_DF  # type: ignore[attr-defined]
    f._weather_df = _WEATHER_DF  # type: ignore[attr-defined]
    f._assets_df = pl.DataFrame(
        {
            "asset_id": ["WIND_001", "SOLAR_001"],
            "asset_type": ["Wind", "Solar"],
            "capacity_mw": [50.0, 100.0],
            "size_category": ["medium", "large"],
            "display_name": ["Wind 001", "Solar 001"],
        }
    )
    result = asset_panel(f)
    assert isinstance(result, pn.Column)
