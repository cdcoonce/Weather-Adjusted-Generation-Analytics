"""Unit tests for ``weather_analytics.dashboard.components.filters``.

Tests exercise the pure-Python state machine of ``Filters`` and the
helper functions that drive it. Panel widget rendering is NOT tested here
(Panel is hard to unit-test in a headless context). We test:

- Default param values on a freshly constructed ``Filters``
- ``initialize()`` populates asset_id objects and date strings from data
- ``asset_type`` watcher resets ``asset_id`` when the selected asset no
  longer belongs to the new type
- ``asset_type`` watcher does NOT reset when "All" is selected
- ``filter_assets_by_type()`` pure-function logic
"""

from __future__ import annotations

import polars as pl
import pytest

from weather_analytics.dashboard.components.filters import (
    Filters,
    filter_assets_by_type,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_ASSETS_DF = pl.DataFrame(
    {
        "asset_id": ["WIND_001", "WIND_002", "SOLAR_001"],
        "asset_type": ["Wind", "Wind", "Solar"],
        "capacity_mw": [50.0, 75.0, 100.0],
        "size_category": ["medium", "large", "large"],
        "display_name": [
            "Wind Asset 001 (50 MW)",
            "Wind Asset 002 (75 MW)",
            "Solar Asset 001 (100 MW)",
        ],
    }
)

_MANIFEST_START = "2025-01-01"
_MANIFEST_END = "2026-04-11"


# ===========================================================================
# filter_assets_by_type — pure function
# ===========================================================================


@pytest.mark.unit
def test_filter_assets_by_type_all_returns_all_ids() -> None:
    result = filter_assets_by_type(_ASSETS_DF, "All")
    assert set(result) == {"All", "WIND_001", "WIND_002", "SOLAR_001"}


@pytest.mark.unit
def test_filter_assets_by_type_wind_returns_only_wind() -> None:
    result = filter_assets_by_type(_ASSETS_DF, "Wind")
    assert set(result) == {"All", "WIND_001", "WIND_002"}
    assert "SOLAR_001" not in result


@pytest.mark.unit
def test_filter_assets_by_type_solar_returns_only_solar() -> None:
    result = filter_assets_by_type(_ASSETS_DF, "Solar")
    assert set(result) == {"All", "SOLAR_001"}
    assert "WIND_001" not in result


@pytest.mark.unit
def test_filter_assets_by_type_empty_df_returns_all_sentinel() -> None:
    empty = pl.DataFrame(
        {
            "asset_id": [],
            "asset_type": [],
            "capacity_mw": [],
            "size_category": [],
            "display_name": [],
        }
    )
    result = filter_assets_by_type(empty, "Wind")
    assert result == ["All"]


@pytest.mark.unit
def test_filter_assets_by_type_always_starts_with_all() -> None:
    result = filter_assets_by_type(_ASSETS_DF, "Wind")
    assert result[0] == "All"


# ===========================================================================
# Filters — default state
# ===========================================================================


@pytest.mark.unit
def test_filters_default_asset_id_is_all() -> None:
    f = Filters()
    assert f.asset_id == "All"


@pytest.mark.unit
def test_filters_default_asset_type_is_all() -> None:
    f = Filters()
    assert f.asset_type == "All"


@pytest.mark.unit
def test_filters_default_date_start_is_empty() -> None:
    f = Filters()
    assert f.date_start == ""


@pytest.mark.unit
def test_filters_default_date_end_is_empty() -> None:
    f = Filters()
    assert f.date_end == ""


# ===========================================================================
# Filters.initialize()
# ===========================================================================


@pytest.mark.unit
def test_filters_initialize_sets_date_range_from_manifest() -> None:
    f = Filters()
    f.initialize(_ASSETS_DF, _MANIFEST_START, _MANIFEST_END)
    assert f.date_start == _MANIFEST_START
    assert f.date_end == _MANIFEST_END


@pytest.mark.unit
def test_filters_initialize_populates_asset_id_objects() -> None:
    f = Filters()
    f.initialize(_ASSETS_DF, _MANIFEST_START, _MANIFEST_END)
    objects = f.param["asset_id"].objects
    assert "All" in objects
    assert "WIND_001" in objects
    assert "SOLAR_001" in objects


@pytest.mark.unit
def test_filters_initialize_keeps_asset_id_as_all() -> None:
    f = Filters()
    f.initialize(_ASSETS_DF, _MANIFEST_START, _MANIFEST_END)
    assert f.asset_id == "All"


# ===========================================================================
# Filters — asset_type watcher (state machine)
# ===========================================================================


@pytest.mark.unit
def test_asset_type_change_resets_asset_id_when_no_longer_valid() -> None:
    """Selecting Wind while a Solar asset is active resets to All."""
    f = Filters()
    f.initialize(_ASSETS_DF, _MANIFEST_START, _MANIFEST_END)
    # Select a solar asset first
    f.asset_id = "SOLAR_001"
    assert f.asset_id == "SOLAR_001"
    # Now switch type to Wind — SOLAR_001 is not in Wind objects
    f.asset_type = "Wind"
    assert f.asset_id == "All"


@pytest.mark.unit
def test_asset_type_change_preserves_asset_id_when_still_valid() -> None:
    """Switching from All to Wind while a Wind asset is active keeps the asset."""
    f = Filters()
    f.initialize(_ASSETS_DF, _MANIFEST_START, _MANIFEST_END)
    f.asset_id = "WIND_001"
    f.asset_type = "Wind"
    # WIND_001 is valid under Wind — should not reset
    assert f.asset_id == "WIND_001"


@pytest.mark.unit
def test_asset_type_change_to_all_preserves_asset_id() -> None:
    """Switching back to All after Wind keeps the wind asset selected."""
    f = Filters()
    f.initialize(_ASSETS_DF, _MANIFEST_START, _MANIFEST_END)
    f.asset_type = "Wind"
    f.asset_id = "WIND_002"
    f.asset_type = "All"
    # WIND_002 is in the "All" object list so it remains valid
    assert f.asset_id == "WIND_002"


@pytest.mark.unit
def test_asset_type_change_repopulates_asset_id_objects() -> None:
    """After switching to Solar, only Solar assets + All appear in objects."""
    f = Filters()
    f.initialize(_ASSETS_DF, _MANIFEST_START, _MANIFEST_END)
    f.asset_type = "Solar"
    objects = f.param["asset_id"].objects
    assert "SOLAR_001" in objects
    assert "WIND_001" not in objects
    assert "All" in objects
