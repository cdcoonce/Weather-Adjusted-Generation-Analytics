"""Unit tests for ``weather_analytics.dashboard.components.fleet_view``.

Tests exercise the pure data-preparation functions extracted from fleet_view:
- ``_apply_fleet_filters`` — joins daily_df with assets_df and applies all filters
- ``_prep_generation_lines`` — returns list of (asset_id, dates, values, color)
- ``_prep_capacity_bars`` — returns (asset_ids, mean_cfs, colors) sorted descending
- ``_prep_heatmap`` — returns filtered heatmap DataFrame for performance scores

``fleet_panel`` is smoke-tested only — Panel rendering is not exercised.

Colour constants are verified against known values.
"""


from datetime import date, datetime, timedelta

import panel as pn
import polars as pl
import pytest

from weather_analytics.dashboard.components.filters import Filters
from weather_analytics.dashboard.components.fleet_view import (
    _ASSET_LINE_PALETTE,
    _FALLBACK_COLOR,
    _SOLAR_COLOR,
    _WIND_COLOR,
    _apply_fleet_filters,
    _asset_color,
    _prep_capacity_bars,
    _prep_generation_lines,
    _prep_heatmap,
    fleet_panel,
)

# ---------------------------------------------------------------------------
# Row counts derived from the fixture data below.
# ---------------------------------------------------------------------------
_TOTAL_ROWS = 6  # 3 assets x 2 dates
_WIND_ROWS = 4  # WIND_001 + WIND_002, 2 dates each
_SOLAR_ROWS = 2  # SOLAR_001, 2 dates
_SINGLE_ASSET_ROWS = 2  # one asset, 2 dates
_SINGLE_DATE_ROWS = 3  # all assets, 1 date

_WIND001_MEAN_CF = 0.45  # (0.40 + 0.50) / 2
_CF_TOLERANCE = 1e-9

_MAX_HEATMAP_DAYS = 90
_LONG_DATE_COUNT = 100

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_ASSETS_DF = pl.DataFrame(
    {
        "asset_id": ["WIND_001", "WIND_002", "SOLAR_001"],
        "asset_type": ["Wind", "Wind", "Solar"],
        "capacity_mw": [50.0, 75.0, 100.0],
        "size_category": ["medium", "large", "large"],
        "display_name": ["Wind 001", "Wind 002", "Solar 001"],
    }
)

_DAILY_DF = pl.DataFrame(
    {
        "asset_id": [
            "WIND_001",
            "WIND_001",
            "WIND_002",
            "WIND_002",
            "SOLAR_001",
            "SOLAR_001",
        ],
        "date": [
            "2025-01-01",
            "2025-01-02",
            "2025-01-01",
            "2025-01-02",
            "2025-01-01",
            "2025-01-02",
        ],
        "total_net_generation_mwh": [100.0, 200.0, 80.0, 160.0, 50.0, 90.0],
        "daily_capacity_factor": [0.40, 0.50, 0.35, 0.45, 0.30, 0.38],
        "avg_availability_pct": [98.0, 99.0, 97.0, 98.5, 96.0, 97.5],
    }
)

_WEATHER_DF = pl.DataFrame(
    {
        "asset_id": [
            "WIND_001",
            "WIND_001",
            "WIND_002",
            "WIND_002",
            "SOLAR_001",
            "SOLAR_001",
        ],
        "date": [
            "2025-01-01",
            "2025-01-02",
            "2025-01-01",
            "2025-01-02",
            "2025-01-01",
            "2025-01-02",
        ],
        "performance_score": [0.80, 0.90, 0.75, 0.85, 0.70, 0.78],
    }
)

_EMPTY_DAILY = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Utf8),
        "total_net_generation_mwh": pl.Series([], dtype=pl.Float64),
        "daily_capacity_factor": pl.Series([], dtype=pl.Float64),
        "avg_availability_pct": pl.Series([], dtype=pl.Float64),
    }
)

_EMPTY_WEATHER = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Utf8),
        "performance_score": pl.Series([], dtype=pl.Float64),
    }
)


# ===========================================================================
# Palette constants
# ===========================================================================


@pytest.mark.unit
def test_wind_color_is_teal() -> None:
    assert _WIND_COLOR == "#4a7c7e"


@pytest.mark.unit
def test_solar_color_is_amber() -> None:
    assert _SOLAR_COLOR == "#d4a44c"


@pytest.mark.unit
def test_fallback_color_is_grey() -> None:
    assert _FALLBACK_COLOR == "#888888"


@pytest.mark.unit
def test_asset_color_wind() -> None:
    assert _asset_color("Wind") == _WIND_COLOR


@pytest.mark.unit
def test_asset_color_solar() -> None:
    assert _asset_color("Solar") == _SOLAR_COLOR


@pytest.mark.unit
def test_asset_color_unknown_is_fallback() -> None:
    assert _asset_color("Unknown") == _FALLBACK_COLOR


# ===========================================================================
# _apply_fleet_filters
# ===========================================================================


@pytest.mark.unit
def test_apply_fleet_filters_no_filters_returns_all_rows() -> None:
    """With all filters at 'All' and no date bounds, every row is returned."""
    result = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "")
    assert result.shape[0] == _TOTAL_ROWS


@pytest.mark.unit
def test_apply_fleet_filters_adds_asset_type_column() -> None:
    """Result DataFrame includes asset_type from the join."""
    result = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "")
    assert "asset_type" in result.columns


@pytest.mark.unit
def test_apply_fleet_filters_by_asset_id() -> None:
    """Filtering to a single asset_id returns only that asset's rows."""
    result = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "WIND_001", "All", "", "")
    assert result.shape[0] == _SINGLE_ASSET_ROWS
    assert set(result["asset_id"].to_list()) == {"WIND_001"}


@pytest.mark.unit
def test_apply_fleet_filters_by_asset_type_wind() -> None:
    """Filtering by asset_type=Wind returns only Wind asset rows."""
    result = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "Wind", "", "")
    # WIND_001 (2 rows) + WIND_002 (2 rows) = 4
    assert result.shape[0] == _WIND_ROWS
    assert set(result["asset_type"].to_list()) == {"Wind"}


@pytest.mark.unit
def test_apply_fleet_filters_by_asset_type_solar() -> None:
    """Filtering by asset_type=Solar returns only Solar asset rows."""
    result = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "Solar", "", "")
    assert result.shape[0] == _SOLAR_ROWS
    assert set(result["asset_type"].to_list()) == {"Solar"}


@pytest.mark.unit
def test_apply_fleet_filters_by_date_start() -> None:
    """date_start filters out rows before the start date."""
    result = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "2025-01-02", "")
    # Only 2025-01-02 rows: 3 assets x 1 date = 3
    assert result.shape[0] == _SINGLE_DATE_ROWS
    assert all(d >= "2025-01-02" for d in result["date"].to_list())


@pytest.mark.unit
def test_apply_fleet_filters_by_date_end() -> None:
    """date_end filters out rows after the end date."""
    result = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "2025-01-01")
    # Only 2025-01-01 rows: 3 assets x 1 date = 3
    assert result.shape[0] == _SINGLE_DATE_ROWS
    assert all(d <= "2025-01-01" for d in result["date"].to_list())


@pytest.mark.unit
def test_apply_fleet_filters_by_date_range() -> None:
    """Combined date range returns only rows within [start, end]."""
    result = _apply_fleet_filters(
        _DAILY_DF, _ASSETS_DF, "All", "All", "2025-01-01", "2025-01-01"
    )
    assert result.shape[0] == _SINGLE_DATE_ROWS
    assert all(d == "2025-01-01" for d in result["date"].to_list())


@pytest.mark.unit
def test_apply_fleet_filters_empty_daily_returns_empty() -> None:
    """Empty daily_df returns an empty DataFrame (with asset_type col)."""
    result = _apply_fleet_filters(_EMPTY_DAILY, _ASSETS_DF, "All", "All", "", "")
    assert result.shape[0] == 0
    assert "asset_type" in result.columns


# ===========================================================================
# _prep_generation_lines
# ===========================================================================


@pytest.mark.unit
def test_prep_generation_lines_returns_one_entry_per_asset() -> None:
    """Returns a list with one tuple per unique asset."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "")
    lines = _prep_generation_lines(filtered)
    asset_ids = [entry[0] for entry in lines]
    assert set(asset_ids) == {"WIND_001", "WIND_002", "SOLAR_001"}


@pytest.mark.unit
def test_prep_generation_lines_dates_are_datetime_objects() -> None:
    """Date values in each line are datetime objects (for Bokeh datetime axis)."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "")
    lines = _prep_generation_lines(filtered)
    for _asset_id, dates, _values, _color in lines:
        for d in dates:
            assert isinstance(d, datetime), f"Expected datetime, got {type(d)}"


@pytest.mark.unit
def test_prep_generation_lines_values_match_daily_mwh() -> None:
    """MWh values for WIND_001 match the fixture data (sorted by date)."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "WIND_001", "All", "", "")
    lines = _prep_generation_lines(filtered)
    assert len(lines) == 1
    _asset_id, _dates, values, _color = lines[0]
    # Sorted by date: 2025-01-01=100, 2025-01-02=200
    assert values == [100.0, 200.0]


@pytest.mark.unit
def test_prep_generation_lines_single_asset_gets_first_palette_color() -> None:
    """A single asset (index 0) gets the first colour in _ASSET_LINE_PALETTE."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "WIND_001", "All", "", "")
    lines = _prep_generation_lines(filtered)
    _asset_id, _dates, _values, color = lines[0]
    assert color == _ASSET_LINE_PALETTE[0]


@pytest.mark.unit
def test_prep_generation_lines_colors_differ_across_assets() -> None:
    """Multiple assets receive distinct colours from the palette."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "")
    lines = _prep_generation_lines(filtered)
    colors = [c for _, _, _, c in lines]
    # At least two distinct assets should have different colours.
    assert len(set(colors)) > 1


@pytest.mark.unit
def test_prep_generation_lines_empty_df_returns_empty_list() -> None:
    """Empty filtered DataFrame returns an empty list."""
    empty_with_type = pl.DataFrame(
        {
            "asset_id": pl.Series([], dtype=pl.Utf8),
            "date": pl.Series([], dtype=pl.Utf8),
            "total_net_generation_mwh": pl.Series([], dtype=pl.Float64),
            "asset_type": pl.Series([], dtype=pl.Utf8),
        }
    )
    lines = _prep_generation_lines(empty_with_type)
    assert lines == []


# ===========================================================================
# _prep_capacity_bars
# ===========================================================================


@pytest.mark.unit
def test_prep_capacity_bars_returns_three_parallel_lists() -> None:
    """Returns a tuple of (asset_ids, mean_cfs, colors) with equal lengths."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "")
    asset_ids, mean_cfs, colors = _prep_capacity_bars(filtered)
    assert len(asset_ids) == len(mean_cfs) == len(colors) == _SINGLE_DATE_ROWS


@pytest.mark.unit
def test_prep_capacity_bars_sorted_descending_by_cf() -> None:
    """Asset list is sorted so highest mean CF is first."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "")
    _asset_ids, mean_cfs, _colors = _prep_capacity_bars(filtered)
    assert mean_cfs == sorted(mean_cfs, reverse=True)


@pytest.mark.unit
def test_prep_capacity_bars_wind001_mean_cf() -> None:
    """WIND_001 mean CF = (0.40 + 0.50) / 2 = 0.45."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "WIND_001", "All", "", "")
    asset_ids, mean_cfs, _colors = _prep_capacity_bars(filtered)
    assert len(asset_ids) == 1
    assert abs(mean_cfs[0] - _WIND001_MEAN_CF) < _CF_TOLERANCE


@pytest.mark.unit
def test_prep_capacity_bars_colors_match_asset_type() -> None:
    """Colors in the result match the expected color for each asset type."""
    filtered = _apply_fleet_filters(_DAILY_DF, _ASSETS_DF, "All", "All", "", "")
    asset_ids, _mean_cfs, colors = _prep_capacity_bars(filtered)
    # Build expected color map from assets_df
    color_map = {
        "WIND_001": _WIND_COLOR,
        "WIND_002": _WIND_COLOR,
        "SOLAR_001": _SOLAR_COLOR,
    }
    for aid, color in zip(asset_ids, colors, strict=False):
        assert color == color_map[aid], f"Wrong color for {aid}"


@pytest.mark.unit
def test_prep_capacity_bars_empty_df_returns_empty_lists() -> None:
    """Empty filtered DataFrame returns three empty lists."""
    empty_with_type = pl.DataFrame(
        {
            "asset_id": pl.Series([], dtype=pl.Utf8),
            "date": pl.Series([], dtype=pl.Utf8),
            "daily_capacity_factor": pl.Series([], dtype=pl.Float64),
            "asset_type": pl.Series([], dtype=pl.Utf8),
        }
    )
    asset_ids, mean_cfs, colors = _prep_capacity_bars(empty_with_type)
    assert asset_ids == []
    assert mean_cfs == []
    assert colors == []


# ===========================================================================
# _prep_heatmap
# ===========================================================================


@pytest.mark.unit
def test_prep_heatmap_returns_dataframe() -> None:
    """Result is a Polars DataFrame."""
    result = _prep_heatmap(_WEATHER_DF, _ASSETS_DF, "All", "All", "", "")
    assert isinstance(result, pl.DataFrame)


@pytest.mark.unit
def test_prep_heatmap_has_required_columns() -> None:
    """Result has asset_id, date, performance_score, and asset_type columns."""
    result = _prep_heatmap(_WEATHER_DF, _ASSETS_DF, "All", "All", "", "")
    for col in ("asset_id", "date", "performance_score", "asset_type"):
        assert col in result.columns


@pytest.mark.unit
def test_prep_heatmap_no_filters_returns_all_rows() -> None:
    """Without filters, all 6 rows are returned."""
    result = _prep_heatmap(_WEATHER_DF, _ASSETS_DF, "All", "All", "", "")
    assert result.shape[0] == _TOTAL_ROWS


@pytest.mark.unit
def test_prep_heatmap_filters_by_asset_id() -> None:
    """Filtering to WIND_001 returns only WIND_001 rows."""
    result = _prep_heatmap(_WEATHER_DF, _ASSETS_DF, "WIND_001", "All", "", "")
    assert result.shape[0] == _SINGLE_ASSET_ROWS
    assert set(result["asset_id"].to_list()) == {"WIND_001"}


@pytest.mark.unit
def test_prep_heatmap_filters_by_asset_type() -> None:
    """Filtering to Wind returns only Wind asset rows."""
    result = _prep_heatmap(_WEATHER_DF, _ASSETS_DF, "All", "Wind", "", "")
    assert result.shape[0] == _WIND_ROWS
    assert set(result["asset_type"].to_list()) == {"Wind"}


@pytest.mark.unit
def test_prep_heatmap_filters_by_date_range() -> None:
    """Date range filter returns only matching rows."""
    result = _prep_heatmap(
        _WEATHER_DF, _ASSETS_DF, "All", "All", "2025-01-02", "2025-01-02"
    )
    assert result.shape[0] == _SINGLE_DATE_ROWS
    assert all(d == "2025-01-02" for d in result["date"].to_list())


@pytest.mark.unit
def test_prep_heatmap_empty_weather_returns_empty_df() -> None:
    """Empty weather_df returns an empty DataFrame."""
    result = _prep_heatmap(_EMPTY_WEATHER, _ASSETS_DF, "All", "All", "", "")
    assert result.shape[0] == 0


@pytest.mark.unit
def test_prep_heatmap_caps_at_90_days() -> None:
    """When the date range spans more than 90 days, only the most recent
    90 distinct dates are returned per asset."""
    start = date(2025, 1, 1)
    date_strs = [
        (start + timedelta(days=i)).isoformat() for i in range(_LONG_DATE_COUNT)
    ]
    long_weather = pl.DataFrame(
        {
            "asset_id": ["WIND_001"] * _LONG_DATE_COUNT,
            "date": date_strs,
            "performance_score": [0.8] * _LONG_DATE_COUNT,
        }
    )
    result = _prep_heatmap(long_weather, _ASSETS_DF, "WIND_001", "All", "", "")
    unique_dates = result["date"].unique().shape[0]
    assert unique_dates <= _MAX_HEATMAP_DAYS


# ===========================================================================
# fleet_panel smoke test
# ===========================================================================


@pytest.mark.unit
def test_fleet_panel_is_importable_and_callable() -> None:
    """fleet_panel must be importable and callable without error."""
    assert callable(fleet_panel)


@pytest.mark.unit
def test_fleet_panel_returns_panel_column() -> None:
    """fleet_panel(filters) returns a pn.Column."""
    f = Filters()
    f._daily_df = _DAILY_DF  # type: ignore[attr-defined]
    f._weather_df = _WEATHER_DF  # type: ignore[attr-defined]
    f._assets_df = _ASSETS_DF  # type: ignore[attr-defined]

    result = fleet_panel(f)
    assert isinstance(result, pn.Column)
