"""Unit tests for ``weather_analytics.dashboard.data_loader``."""


import asyncio
import json

import polars as pl
import pytest

from weather_analytics.dashboard.data_loader import (
    EXPECTED_SCHEMA_VERSION,
    Manifest,
    clear_cache,
    load_assets,
    load_daily_performance,
    load_json,
    load_manifest,
    load_weather_performance,
)

_FIXTURE_ASSET_COUNT = 10
_FIXTURE_ROW_COUNT = 3650

MANIFEST_FIXTURE = {
    "generated_at": "2026-04-12T10:00:00Z",
    "pipeline_run_id": "run-abc",
    "date_range": {"start": "2025-01-01", "end": "2026-04-11"},
    "asset_count": _FIXTURE_ASSET_COUNT,
    "row_counts": {"daily_performance": _FIXTURE_ROW_COUNT},
    "schema_version": "1.0",
}


DAILY_FIXTURE = [
    {
        "asset_id": "ASSET_001",
        "date": "2026-04-10",
        "total_net_generation_mwh": 120.5,
        "daily_capacity_factor": 0.41,
        "avg_availability_pct": 98.0,
        "total_curtailment_mwh": 2.5,
        "daily_performance_rating": "good",
        "excellent_hours": 4,
        "good_hours": 10,
        "fair_hours": 6,
        "poor_hours": 4,
        "avg_wind_speed_mps": 7.2,
        "avg_ghi": 450.0,
        "avg_temperature_c": 18.5,
        "data_completeness_pct": 100.0,
    },
    {
        "asset_id": "ASSET_002",
        "date": "2026-04-10",
        "total_net_generation_mwh": 200.0,
        "daily_capacity_factor": 0.45,
        "avg_availability_pct": 99.0,
        "total_curtailment_mwh": 0.0,
        "daily_performance_rating": "excellent",
        "excellent_hours": 12,
        "good_hours": 8,
        "fair_hours": 4,
        "poor_hours": 0,
        "avg_wind_speed_mps": 8.5,
        "avg_ghi": 500.0,
        "avg_temperature_c": 19.0,
        "data_completeness_pct": 100.0,
    },
]

_DAILY_EXPECTED_COLUMNS = {
    "asset_id",
    "date",
    "total_net_generation_mwh",
    "daily_capacity_factor",
    "avg_availability_pct",
    "total_curtailment_mwh",
    "daily_performance_rating",
    "excellent_hours",
    "good_hours",
    "fair_hours",
    "poor_hours",
    "avg_wind_speed_mps",
    "avg_ghi",
    "avg_temperature_c",
    "data_completeness_pct",
}

ASSETS_FIXTURE = [
    {
        "asset_id": "WIND_001",
        "asset_type": "wind",
        "capacity_mw": 50.0,
        "size_category": "medium",
        "display_name": "Wind Asset 001 (50 MW)",
    },
    {
        "asset_id": "SOLAR_002",
        "asset_type": "solar",
        "capacity_mw": 75.0,
        "size_category": "large",
        "display_name": "Solar Asset 002 (75 MW)",
    },
]

WEATHER_PERFORMANCE_FIXTURE = [
    {
        "asset_id": "WIND_001",
        "date": "2026-04-10",
        "performance_score": 0.85,
        "performance_category": "good",
        "avg_expected_generation_mwh": 100.0,
        "avg_actual_generation_mwh": 85.0,
        "avg_performance_ratio_pct": 85.0,
        "wind_r_squared": 0.92,
        "solar_r_squared": None,
        "inferred_asset_type": "wind",
        "rolling_7d_avg_cf": 0.40,
        "rolling_30d_avg_cf": 0.38,
    },
]


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    """Ensure no cross-test cache leakage."""
    clear_cache()
    yield
    clear_cache()


# ===========================================================================
# Manifest.from_dict
# ===========================================================================


@pytest.mark.unit
def test_manifest_from_dict_parses_fields() -> None:
    manifest = Manifest.from_dict(MANIFEST_FIXTURE)
    assert manifest.generated_at == "2026-04-12T10:00:00Z"
    assert manifest.pipeline_run_id == "run-abc"
    assert manifest.date_range_start == "2025-01-01"
    assert manifest.date_range_end == "2026-04-11"
    assert manifest.asset_count == _FIXTURE_ASSET_COUNT
    assert manifest.row_counts == {"daily_performance": _FIXTURE_ROW_COUNT}
    assert manifest.schema_version == "1.0"


@pytest.mark.unit
def test_manifest_schema_matches() -> None:
    manifest = Manifest.from_dict(MANIFEST_FIXTURE)
    assert manifest.schema_matches is True


@pytest.mark.unit
def test_manifest_schema_mismatch_detected() -> None:
    data = dict(MANIFEST_FIXTURE)
    data["schema_version"] = "2.0"
    manifest = Manifest.from_dict(data)
    assert manifest.schema_matches is False


@pytest.mark.unit
def test_manifest_from_dict_defaults() -> None:
    """Missing optional fields default gracefully, required ones raise."""
    minimal = {
        "generated_at": "2026-04-12T10:00:00Z",
        "date_range": {"start": "2025-01-01", "end": "2026-04-11"},
        "asset_count": 1,
    }
    manifest = Manifest.from_dict(minimal)
    assert manifest.pipeline_run_id == ""
    assert manifest.row_counts == {}
    assert manifest.schema_version == ""
    assert manifest.schema_matches is False


@pytest.mark.unit
def test_expected_schema_version_constant() -> None:
    """Guard: if someone changes the constant they must also update the app."""
    assert EXPECTED_SCHEMA_VERSION == "1.0"


# ===========================================================================
# load_json / load_manifest / load_daily_performance
# ===========================================================================


async def _run_load_json(monkeypatch: pytest.MonkeyPatch, payload: str) -> object:
    """Helper: stub _fetch_text and call load_json once."""

    async def _fake_fetch(url: str) -> str:
        return payload

    monkeypatch.setattr(
        "weather_analytics.dashboard.data_loader._fetch_text",
        _fake_fetch,
    )
    return await load_json("daily_performance.json", base="./data")


@pytest.mark.unit
def test_load_json_parses_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Second call must not re-fetch — result must come from the cache."""
    calls = {"n": 0}

    async def _fake_fetch(url: str) -> str:
        calls["n"] += 1
        return json.dumps(DAILY_FIXTURE)

    monkeypatch.setattr(
        "weather_analytics.dashboard.data_loader._fetch_text",
        _fake_fetch,
    )

    first = asyncio.run(load_json("daily_performance.json"))
    second = asyncio.run(load_json("daily_performance.json"))
    assert first == DAILY_FIXTURE
    assert second == DAILY_FIXTURE
    assert calls["n"] == 1


@pytest.mark.unit
def test_load_json_raises_on_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_fetch(url: str) -> str:
        msg = "fetch failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        "weather_analytics.dashboard.data_loader._fetch_text",
        _fake_fetch,
    )

    with pytest.raises(RuntimeError, match="fetch failed"):
        asyncio.run(load_json("daily_performance.json"))


@pytest.mark.unit
def test_load_manifest_constructs_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_fetch(url: str) -> str:
        return json.dumps(MANIFEST_FIXTURE)

    monkeypatch.setattr(
        "weather_analytics.dashboard.data_loader._fetch_text",
        _fake_fetch,
    )
    manifest = asyncio.run(load_manifest())
    assert isinstance(manifest, Manifest)
    assert manifest.schema_matches is True


@pytest.mark.unit
def test_load_daily_performance_returns_polars_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_fetch(url: str) -> str:
        return json.dumps(DAILY_FIXTURE)

    monkeypatch.setattr(
        "weather_analytics.dashboard.data_loader._fetch_text",
        _fake_fetch,
    )
    df = asyncio.run(load_daily_performance())
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, len(_DAILY_EXPECTED_COLUMNS))
    assert set(df.columns) == _DAILY_EXPECTED_COLUMNS


@pytest.mark.unit
def test_load_assets_returns_polars_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_fetch(url: str) -> str:
        return json.dumps(ASSETS_FIXTURE)

    monkeypatch.setattr(
        "weather_analytics.dashboard.data_loader._fetch_text",
        _fake_fetch,
    )
    df = asyncio.run(load_assets())
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == len(ASSETS_FIXTURE)
    assert set(df.columns) == {
        "asset_id",
        "asset_type",
        "capacity_mw",
        "size_category",
        "display_name",
    }


@pytest.mark.unit
def test_load_weather_performance_returns_polars_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_fetch(url: str) -> str:
        return json.dumps(WEATHER_PERFORMANCE_FIXTURE)

    monkeypatch.setattr(
        "weather_analytics.dashboard.data_loader._fetch_text",
        _fake_fetch,
    )
    df = asyncio.run(load_weather_performance())
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == len(WEATHER_PERFORMANCE_FIXTURE)
    assert "asset_id" in df.columns
    assert "performance_score" in df.columns
