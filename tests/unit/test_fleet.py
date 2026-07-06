"""Unit tests for the fleet registry."""

from __future__ import annotations

import pytest

from weather_analytics.mock_data.fleet import (
    ASSET_TYPES,
    BATTERY,
    FLEET,
    FLEET_BY_ID,
    GAS,
    SOLAR,
    WIND,
    FleetAsset,
    assets_of_type,
)

pytestmark = pytest.mark.unit


def test_fleet_has_all_four_technologies() -> None:
    types = {a.asset_type for a in FLEET}
    assert types == set(ASSET_TYPES)


def test_fleet_type_counts() -> None:
    counts = {t: len(assets_of_type(t)) for t in ASSET_TYPES}
    assert counts == {WIND: 4, SOLAR: 4, BATTERY: 2, GAS: 2}


def test_each_asset_has_exactly_one_params_object() -> None:
    for a in FLEET:
        populated = [p for p in (a.wind, a.solar, a.battery, a.gas) if p is not None]
        assert len(populated) == 1, a.asset_id
        # The populated params must match the declared type.
        expected = {WIND: a.wind, SOLAR: a.solar, BATTERY: a.battery, GAS: a.gas}
        assert expected[a.asset_type] is not None


def test_asset_ids_unique_and_indexed() -> None:
    ids = [a.asset_id for a in FLEET]
    assert len(ids) == len(set(ids))
    assert set(FLEET_BY_ID) == set(ids)


def test_coordinates_are_plausible_us_sites() -> None:
    for a in FLEET:
        assert 24.0 <= a.latitude <= 49.0, a.asset_id
        assert -125.0 <= a.longitude <= -66.0, a.asset_id


@pytest.mark.parametrize(
    ("mw", "expected"),
    [(80.0, "Large"), (75.0, "Large"), (60.0, "Medium"), (50.0, "Medium"),
     (49.0, "Small"), (30.0, "Small")],
)
def test_size_category_buckets(mw: float, expected: str) -> None:
    asset = FleetAsset("X", "Test", WIND, mw, 40.0, -100.0, "TEST")
    assert asset.size_category == expected


def test_display_name_includes_capacity_and_type() -> None:
    asset = FleetAsset("X", "Roscoe Ridge", WIND, 150.0, 32.0, -100.0, "ERCOT")
    assert asset.display_name == "Roscoe Ridge (150 MW wind)"
