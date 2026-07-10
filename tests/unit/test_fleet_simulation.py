"""Unit tests for the fleet simulation and local dashboard export.

All offline: uses synthetic weather so there is no network dependency.
"""

from __future__ import annotations

import polars as pl
import pytest

from weather_analytics.mock_data.fleet import FLEET
from weather_analytics.mock_data.local_export import (
    SCHEMA_VERSION,
    build_bundle,
    build_local_exports,
)
from weather_analytics.mock_data.simulate import GENERATION_COLUMNS, simulate_fleet

pytestmark = pytest.mark.unit

_START = "2026-01-01T00:00:00"
_END = "2026-01-31T23:00:00"  # 31 days


@pytest.fixture(scope="module")
def sim():
    return simulate_fleet(_START, _END, use_real_weather=False, random_seed=3)


def test_generation_schema_and_row_count(sim) -> None:
    g = sim.generation
    assert tuple(g.columns) == GENERATION_COLUMNS
    hours = g.select(pl.col("timestamp").n_unique()).item()
    assert g.height == hours * len(FLEET)
    assert sim.weather_source == "synthetic"


def test_battery_rows_carry_soc_and_can_go_negative(sim) -> None:
    battery = sim.generation.filter(pl.col("asset_type") == "battery")
    assert battery["soc_pct"].null_count() == 0
    # Storage net energy over a period is negative (round-trip losses).
    assert battery["net_generation_mwh"].sum() < 0


def test_wind_solar_rows_have_null_storage_fields(sim) -> None:
    ws = sim.generation.filter(pl.col("asset_type").is_in(["wind", "solar"]))
    assert ws["soc_pct"].null_count() == ws.height
    assert ws["fuel_mmbtu"].null_count() == ws.height


def test_gas_rows_have_fuel_and_co2(sim) -> None:
    gas = sim.generation.filter(pl.col("asset_type") == "gas")
    running = gas.filter(pl.col("net_generation_mwh") > 0)
    assert running["co2_tonnes"].fill_null(0).sum() > 0
    assert running["fuel_mmbtu"].fill_null(0).sum() > 0


def test_capacity_factors_in_realistic_ranges(sim) -> None:
    g = sim.generation
    hours = g.select(pl.col("timestamp").n_unique()).item()
    agg = g.group_by("asset_id").agg(
        pl.col("net_generation_mwh").sum().alias("net"),
        pl.col("asset_type").first(),
        pl.col("asset_capacity_mw").first(),
    )
    for r in agg.iter_rows(named=True):
        cf = r["net"] / (r["asset_capacity_mw"] * hours)
        if r["asset_type"] == "wind":
            assert 0.10 <= cf <= 0.55
        elif r["asset_type"] == "solar":
            assert 0.05 <= cf <= 0.40
        elif r["asset_type"] == "gas":
            assert 0.0 <= cf <= 0.75


def test_build_bundle_produces_all_payloads(sim) -> None:
    bundle = build_bundle(sim, FLEET)
    n_days = sim.generation.select(pl.col("timestamp").dt.date().n_unique()).item()
    assert len(bundle.assets) == len(FLEET)
    assert len(bundle.daily) == len(FLEET) * n_days
    assert len(bundle.weather) == len(FLEET) * n_days
    assert bundle.manifest["schema_version"] == SCHEMA_VERSION
    assert bundle.manifest["asset_type_counts"] == {
        "wind": 4, "solar": 4, "battery": 2, "gas": 2,
    }


def test_daily_export_has_type_specific_columns(sim) -> None:
    bundle = build_bundle(sim, FLEET)
    keys = set(bundle.daily[0])
    for col in (
        "asset_type", "avg_soc_pct", "total_discharge_mwh", "total_co2_tonnes",
        "daily_capacity_factor", "total_net_generation_mwh",
    ):
        assert col in keys


def test_performance_scores_bounded(sim) -> None:
    bundle = build_bundle(sim, FLEET)
    scores = [r["performance_score"] for r in bundle.weather
              if r["performance_score"] is not None]
    assert scores
    assert all(0.0 <= s <= 100.0 for s in scores)


def test_inferred_asset_type_uses_actual_type(sim) -> None:
    bundle = build_bundle(sim, FLEET)
    types = {r["inferred_asset_type"] for r in bundle.weather}
    assert types == {"wind", "solar", "battery", "gas"}


def test_build_local_exports_writes_four_files(tmp_path) -> None:
    manifest = build_local_exports(
        _START, _END, tmp_path, use_real_weather=False, random_seed=3
    )
    for name in ("manifest.json", "assets.json", "daily_performance.json",
                 "weather_performance.json"):
        assert (tmp_path / name).exists()
    assert manifest["weather_source"] == "synthetic"
    assert manifest["asset_count"] == len(FLEET)


def test_warmup_equals_wide_window_filtered() -> None:
    """warmup_days=K is exactly a K-day-earlier window filtered to [start, end]."""
    from datetime import datetime

    warm = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        random_seed=7,
        warmup_days=7,
    )
    wide = simulate_fleet(
        "2023-06-08T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        random_seed=7,
    )
    expected = wide.generation.filter(
        pl.col("timestamp") >= datetime(2023, 6, 15)
    )
    assert warm.generation.equals(expected)


def test_warmup_output_bounded_to_requested_window() -> None:
    from datetime import datetime

    result = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        warmup_days=7,
    )
    for frame in (result.generation, result.weather):
        assert frame["timestamp"].min() == datetime(2023, 6, 15, 0)
        assert frame["timestamp"].max() == datetime(2023, 6, 15, 23)


def test_warmup_battery_soc_not_reset_to_half() -> None:
    """With warm-up, the target day's first battery SOC is off the 50% cold start."""
    result = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        warmup_days=7,
    )
    first_soc = (
        result.generation.filter(pl.col("asset_type") == "battery")
        .sort(["asset_id", "timestamp"])
        .group_by("asset_id", maintain_order=True)
        .first()["soc_pct"]
    )
    assert all(abs(v - 50.0) > 0.5 for v in first_soc)


def test_battery_net_negative_over_multiday_window() -> None:
    """Round-trip + parasitic losses make battery net generation negative overall."""
    result = simulate_fleet(
        "2023-06-01T00:00:00",
        "2023-06-14T23:00:00",
        FLEET,
        use_real_weather=False,
    )
    battery_net = (
        result.generation.filter(pl.col("asset_type") == "battery")
        .select(pl.col("net_generation_mwh").sum())
        .item()
    )
    assert battery_net < 0.0


def test_weather_seed_independent_of_physics_seed() -> None:
    """Same weather_seed => identical weather even when random_seed differs."""
    a = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        random_seed=1,
    )
    b = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        random_seed=2,
    )
    assert a.weather.equals(b.weather)
    assert not a.generation.equals(b.generation)


def test_generate_generation_data_warmup_is_idempotent_and_bounded() -> None:
    from datetime import datetime

    from weather_analytics.mock_data.generate_generation import (
        generate_generation_data,
    )

    a = generate_generation_data(
        "2023-06-15T00:00:00", "2023-06-15T23:00:00", random_seed=99, warmup_days=7
    )
    b = generate_generation_data(
        "2023-06-15T00:00:00", "2023-06-15T23:00:00", random_seed=99, warmup_days=7
    )
    assert a.equals(b)
    assert a["timestamp"].min() == datetime(2023, 6, 15, 0)
    assert a["timestamp"].max() == datetime(2023, 6, 15, 23)
