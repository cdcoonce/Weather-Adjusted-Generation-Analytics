"""Unit tests for ``weather_analytics.dashboard.components.weather_view``.

Tests exercise the pure data-preparation functions extracted from weather_view:
- ``_prep_r2_bars`` — aggregate wind/solar R² per asset
- ``_prep_wind_scatter`` — filter daily_df to wind assets for scatter
- ``_prep_solar_scatter`` — filter daily_df to solar assets for scatter

``weather_panel`` is smoke-tested only — Panel rendering is not exercised.
"""

from __future__ import annotations

import panel as pn
import polars as pl
import pytest

from weather_analytics.dashboard.components.filters import Filters
from weather_analytics.dashboard.components.weather_view import (
    _prep_r2_bars,
    _prep_solar_scatter,
    _prep_wind_scatter,
    weather_panel,
)

# ---------------------------------------------------------------------------
# Named constants (avoids PLR2004 magic-value warnings)
# ---------------------------------------------------------------------------
_TOLERANCE = 1e-9
_ALL_ASSETS_COUNT = 3
_WIND_ASSET_COUNT = 2
_WIND001_ROWS = 3
_WIND_ALL_ROWS = 5
_SOLAR_ALL_ROWS = 2
_ONE_ROW = 1
_TWO_ROWS = 2
_WIND001_MEAN_R2 = 0.85
_SOLAR001_MEAN_R2 = 0.685
_WIND001_MEAN_R2_FROM_JAN02 = 0.875
_WIND001_MEAN_R2_TO_JAN01 = 0.80

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_ASSETS_DF = pl.DataFrame(
    {
        "asset_id": ["WIND_001", "WIND_002", "SOLAR_001"],
        "asset_type": ["Wind", "Wind", "Solar"],
        "capacity_mw": [50.0, 60.0, 100.0],
        "size_category": ["medium", "medium", "large"],
        "display_name": ["Wind 001", "Wind 002", "Solar 001"],
    }
)

_WEATHER_DF = pl.DataFrame(
    {
        "asset_id": [
            "WIND_001",
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
            "2025-01-03",
            "2025-01-01",
            "2025-01-02",
            "2025-01-01",
            "2025-01-02",
        ],
        "wind_r_squared": [0.80, 0.85, 0.90, 0.70, 0.75, 0.0, 0.0],
        "solar_r_squared": [0.0, 0.0, 0.0, 0.0, 0.0, 0.65, 0.72],
        "inferred_asset_type": [
            "Wind",
            "Wind",
            "Wind",
            "Wind",
            "Wind",
            "Solar",
            "Solar",
        ],
    }
)

_DAILY_DF = pl.DataFrame(
    {
        "asset_id": [
            "WIND_001",
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
            "2025-01-03",
            "2025-01-01",
            "2025-01-02",
            "2025-01-01",
            "2025-01-02",
        ],
        "total_net_generation_mwh": [100.0, 200.0, 150.0, 80.0, 90.0, 50.0, 60.0],
        "avg_wind_speed_mps": [8.0, 10.0, 9.0, 7.0, 8.5, 0.0, 0.0],
        "avg_ghi": [0.0, 0.0, 0.0, 0.0, 0.0, 250.0, 300.0],
        "avg_temperature_c": [15.0, 16.0, 14.0, 14.0, 15.0, 20.0, 22.0],
    }
)

_EMPTY_ASSETS = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "asset_type": pl.Series([], dtype=pl.Utf8),
        "capacity_mw": pl.Series([], dtype=pl.Float64),
        "size_category": pl.Series([], dtype=pl.Utf8),
        "display_name": pl.Series([], dtype=pl.Utf8),
    }
)

_EMPTY_WEATHER = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Utf8),
        "wind_r_squared": pl.Series([], dtype=pl.Float64),
        "solar_r_squared": pl.Series([], dtype=pl.Float64),
        "inferred_asset_type": pl.Series([], dtype=pl.Utf8),
    }
)

_EMPTY_DAILY = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Utf8),
        "total_net_generation_mwh": pl.Series([], dtype=pl.Float64),
        "avg_wind_speed_mps": pl.Series([], dtype=pl.Float64),
        "avg_ghi": pl.Series([], dtype=pl.Float64),
        "avg_temperature_c": pl.Series([], dtype=pl.Float64),
    }
)


# ===========================================================================
# _prep_r2_bars
# ===========================================================================


@pytest.mark.unit
def test_prep_r2_bars_returns_one_row_per_asset() -> None:
    """Aggregating weather_df returns one row per asset (not one per date)."""
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "All", "All", "", "")
    assert result.shape[0] == _ALL_ASSETS_COUNT


@pytest.mark.unit
def test_prep_r2_bars_has_required_columns() -> None:
    """Result DataFrame has asset_id, mean_wind_r2, mean_solar_r2 columns."""
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "All", "All", "", "")
    assert "asset_id" in result.columns
    assert "mean_wind_r2" in result.columns
    assert "mean_solar_r2" in result.columns


@pytest.mark.unit
def test_prep_r2_bars_wind001_mean_r2() -> None:
    """WIND_001 mean wind R² equals average of [0.80, 0.85, 0.90] = 0.85."""
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "All", "All", "", "")
    row = result.filter(pl.col("asset_id") == "WIND_001")
    assert row.shape[0] == 1
    assert abs(row["mean_wind_r2"][0] - _WIND001_MEAN_R2) < _TOLERANCE


@pytest.mark.unit
def test_prep_r2_bars_solar001_mean_r2() -> None:
    """SOLAR_001 mean solar R² equals average of [0.65, 0.72] = 0.685."""
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "All", "All", "", "")
    row = result.filter(pl.col("asset_id") == "SOLAR_001")
    assert row.shape[0] == 1
    assert abs(row["mean_solar_r2"][0] - _SOLAR001_MEAN_R2) < _TOLERANCE


@pytest.mark.unit
def test_prep_r2_bars_filters_by_asset_id() -> None:
    """When asset_id is not 'All', only that asset's rows are returned."""
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "WIND_001", "All", "", "")
    assert result.shape[0] == 1
    assert result["asset_id"][0] == "WIND_001"


@pytest.mark.unit
def test_prep_r2_bars_filters_by_asset_type_wind() -> None:
    """When asset_type is 'Wind', only wind assets are returned."""
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "All", "Wind", "", "")
    assert result.shape[0] == _WIND_ASSET_COUNT
    assert all(aid in ["WIND_001", "WIND_002"] for aid in result["asset_id"].to_list())


@pytest.mark.unit
def test_prep_r2_bars_filters_by_asset_type_solar() -> None:
    """When asset_type is 'Solar', only solar assets are returned."""
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "All", "Solar", "", "")
    assert result.shape[0] == 1
    assert result["asset_id"][0] == "SOLAR_001"


@pytest.mark.unit
def test_prep_r2_bars_filters_by_date_start() -> None:
    """date_start removes weather rows before the cutoff before aggregating."""
    # Only dates >= 2025-01-02 → WIND_001 mean = avg(0.85, 0.90) = 0.875
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "WIND_001", "All", "2025-01-02", "")
    row = result.filter(pl.col("asset_id") == "WIND_001")
    assert abs(row["mean_wind_r2"][0] - _WIND001_MEAN_R2_FROM_JAN02) < _TOLERANCE


@pytest.mark.unit
def test_prep_r2_bars_filters_by_date_end() -> None:
    """date_end removes weather rows after the cutoff before aggregating."""
    # Only dates <= 2025-01-01 → WIND_001 mean = 0.80
    result = _prep_r2_bars(_WEATHER_DF, _ASSETS_DF, "WIND_001", "All", "", "2025-01-01")
    row = result.filter(pl.col("asset_id") == "WIND_001")
    assert abs(row["mean_wind_r2"][0] - _WIND001_MEAN_R2_TO_JAN01) < _TOLERANCE


@pytest.mark.unit
def test_prep_r2_bars_empty_weather_returns_empty() -> None:
    """Empty weather_df returns empty DataFrame with required columns."""
    result = _prep_r2_bars(_EMPTY_WEATHER, _ASSETS_DF, "All", "All", "", "")
    assert result.shape[0] == 0
    assert "asset_id" in result.columns
    assert "mean_wind_r2" in result.columns
    assert "mean_solar_r2" in result.columns


# ===========================================================================
# _prep_wind_scatter
# ===========================================================================


@pytest.mark.unit
def test_prep_wind_scatter_returns_only_wind_assets() -> None:
    """Result contains only rows from assets with asset_type == 'Wind'."""
    result = _prep_wind_scatter(_DAILY_DF, _ASSETS_DF, "All", "", "")
    asset_ids = set(result["asset_id"].to_list())
    assert "SOLAR_001" not in asset_ids
    assert asset_ids == {"WIND_001", "WIND_002"}


@pytest.mark.unit
def test_prep_wind_scatter_has_required_columns() -> None:
    """Result DataFrame has asset_id, avg_wind_speed_mps, total_net_generation_mwh."""
    result = _prep_wind_scatter(_DAILY_DF, _ASSETS_DF, "All", "", "")
    assert "asset_id" in result.columns
    assert "avg_wind_speed_mps" in result.columns
    assert "total_net_generation_mwh" in result.columns


@pytest.mark.unit
def test_prep_wind_scatter_row_count() -> None:
    """Total rows = rows for all wind assets combined (3 + 2 = 5)."""
    result = _prep_wind_scatter(_DAILY_DF, _ASSETS_DF, "All", "", "")
    assert result.shape[0] == _WIND_ALL_ROWS


@pytest.mark.unit
def test_prep_wind_scatter_filters_by_specific_asset_id() -> None:
    """When asset_id is not 'All', only that asset's rows are returned."""
    result = _prep_wind_scatter(_DAILY_DF, _ASSETS_DF, "WIND_001", "", "")
    assert result.shape[0] == _WIND001_ROWS
    assert set(result["asset_id"].to_list()) == {"WIND_001"}


@pytest.mark.unit
def test_prep_wind_scatter_filters_by_date_start() -> None:
    """date_start removes rows before the cutoff."""
    result = _prep_wind_scatter(_DAILY_DF, _ASSETS_DF, "WIND_001", "2025-01-02", "")
    assert all(d >= "2025-01-02" for d in result["date"].to_list())
    assert result.shape[0] == _TWO_ROWS


@pytest.mark.unit
def test_prep_wind_scatter_filters_by_date_end() -> None:
    """date_end removes rows after the cutoff."""
    result = _prep_wind_scatter(_DAILY_DF, _ASSETS_DF, "WIND_001", "", "2025-01-02")
    assert all(d <= "2025-01-02" for d in result["date"].to_list())
    assert result.shape[0] == _TWO_ROWS


@pytest.mark.unit
def test_prep_wind_scatter_empty_daily_returns_empty() -> None:
    """Empty daily_df returns empty DataFrame with required columns."""
    result = _prep_wind_scatter(_EMPTY_DAILY, _ASSETS_DF, "All", "", "")
    assert result.shape[0] == 0
    assert "asset_id" in result.columns
    assert "avg_wind_speed_mps" in result.columns
    assert "total_net_generation_mwh" in result.columns


@pytest.mark.unit
def test_prep_wind_scatter_no_wind_assets_returns_empty() -> None:
    """When assets_df has no wind assets, result is empty."""
    solar_only_assets = pl.DataFrame(
        {
            "asset_id": ["SOLAR_001"],
            "asset_type": ["Solar"],
            "capacity_mw": [100.0],
            "size_category": ["large"],
            "display_name": ["Solar 001"],
        }
    )
    result = _prep_wind_scatter(_DAILY_DF, solar_only_assets, "All", "", "")
    assert result.shape[0] == 0


# ===========================================================================
# _prep_solar_scatter
# ===========================================================================


@pytest.mark.unit
def test_prep_solar_scatter_returns_only_solar_assets() -> None:
    """Result contains only rows from assets with asset_type == 'Solar'."""
    result = _prep_solar_scatter(_DAILY_DF, _ASSETS_DF, "All", "", "")
    asset_ids = set(result["asset_id"].to_list())
    assert "WIND_001" not in asset_ids
    assert "WIND_002" not in asset_ids
    assert asset_ids == {"SOLAR_001"}


@pytest.mark.unit
def test_prep_solar_scatter_has_required_columns() -> None:
    """Result DataFrame has asset_id, avg_ghi, total_net_generation_mwh."""
    result = _prep_solar_scatter(_DAILY_DF, _ASSETS_DF, "All", "", "")
    assert "asset_id" in result.columns
    assert "avg_ghi" in result.columns
    assert "total_net_generation_mwh" in result.columns


@pytest.mark.unit
def test_prep_solar_scatter_row_count() -> None:
    """Total rows = rows for all solar assets combined (2)."""
    result = _prep_solar_scatter(_DAILY_DF, _ASSETS_DF, "All", "", "")
    assert result.shape[0] == _SOLAR_ALL_ROWS


@pytest.mark.unit
def test_prep_solar_scatter_filters_by_date_start() -> None:
    """date_start removes rows before the cutoff."""
    result = _prep_solar_scatter(_DAILY_DF, _ASSETS_DF, "All", "2025-01-02", "")
    assert all(d >= "2025-01-02" for d in result["date"].to_list())
    assert result.shape[0] == _ONE_ROW


@pytest.mark.unit
def test_prep_solar_scatter_filters_by_date_end() -> None:
    """date_end removes rows after the cutoff."""
    result = _prep_solar_scatter(_DAILY_DF, _ASSETS_DF, "All", "", "2025-01-01")
    assert all(d <= "2025-01-01" for d in result["date"].to_list())
    assert result.shape[0] == _ONE_ROW


@pytest.mark.unit
def test_prep_solar_scatter_empty_daily_returns_empty() -> None:
    """Empty daily_df returns empty DataFrame with required columns."""
    result = _prep_solar_scatter(_EMPTY_DAILY, _ASSETS_DF, "All", "", "")
    assert result.shape[0] == 0
    assert "asset_id" in result.columns
    assert "avg_ghi" in result.columns
    assert "total_net_generation_mwh" in result.columns


@pytest.mark.unit
def test_prep_solar_scatter_no_solar_assets_returns_empty() -> None:
    """When assets_df has no solar assets, result is empty."""
    wind_only_assets = pl.DataFrame(
        {
            "asset_id": ["WIND_001"],
            "asset_type": ["Wind"],
            "capacity_mw": [50.0],
            "size_category": ["medium"],
            "display_name": ["Wind 001"],
        }
    )
    result = _prep_solar_scatter(_DAILY_DF, wind_only_assets, "All", "", "")
    assert result.shape[0] == 0


# ===========================================================================
# weather_panel smoke test
# ===========================================================================


@pytest.mark.unit
def test_weather_panel_is_importable_and_callable() -> None:
    """weather_panel must be importable and callable without error."""
    assert callable(weather_panel)


@pytest.mark.unit
def test_weather_panel_returns_panel_column() -> None:
    """weather_panel(filters) returns a pn.Column."""
    f = Filters()
    f._daily_df = _DAILY_DF  # type: ignore[attr-defined]
    f._weather_df = _WEATHER_DF  # type: ignore[attr-defined]
    f._assets_df = _ASSETS_DF  # type: ignore[attr-defined]
    result = weather_panel(f)
    assert isinstance(result, pn.Column)
