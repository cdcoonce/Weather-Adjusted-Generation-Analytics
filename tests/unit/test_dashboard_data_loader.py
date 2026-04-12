"""Unit tests for ``weather_analytics.dashboard.data_loader``."""

from __future__ import annotations

import asyncio
import json

import polars as pl
import pytest

from weather_analytics.dashboard.data_loader import (
    EXPECTED_SCHEMA_VERSION,
    Manifest,
    clear_cache,
    load_daily_performance,
    load_json,
    load_manifest,
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
    },
    {
        "asset_id": "ASSET_002",
        "date": "2026-04-10",
        "total_net_generation_mwh": 200.0,
        "daily_capacity_factor": 0.45,
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
    assert df.shape == (2, 4)
    assert set(df.columns) == {
        "asset_id",
        "date",
        "total_net_generation_mwh",
        "daily_capacity_factor",
    }
