"""Pytest configuration and shared fixtures."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest


@pytest.fixture
def make_fake_dlt():
    """Factory for the fake dlt resource/pipeline pair the ingestion assets use.

    Returns (resource, pipeline); customize the load id, failure flag, or
    schema_update the fake load_info reports.
    """
    def _make(
        load_id: str = "load_123",
        has_failed_jobs: bool = False,
        schema_update: dict | None = None,
    ):
        load_info = SimpleNamespace(
            loads_ids=[load_id],
            has_failed_jobs=has_failed_jobs,
            load_packages=[
                SimpleNamespace(
                    schema_update=schema_update or {},
                    jobs={"completed_jobs": []},
                )
            ],
        )
        pipeline = MagicMock()
        pipeline.run.return_value = load_info
        resource = MagicMock()
        resource.create_pipeline.return_value = pipeline
        return resource, pipeline
    return _make


@pytest.fixture
def weather_df() -> pl.DataFrame:
    """Small deterministic weather DataFrame for tests."""
    return pl.DataFrame(
        {
            "asset_id": ["ASSET_001", "ASSET_001"],
            "timestamp": ["2023-01-01T00:00:00", "2023-01-01T01:00:00"],
            "wind_speed_mps": [5.0, 6.0],
            "wind_direction_deg": [180.0, 190.0],
            "ghi": [200.0, 210.0],
            "temperature_c": [15.0, 15.5],
            "pressure_hpa": [1013.25, 1013.0],
            "relative_humidity": [50.0, 49.0],
        }
    )


@pytest.fixture
def generation_df() -> pl.DataFrame:
    """Small deterministic generation DataFrame for tests."""
    return pl.DataFrame(
        {
            "asset_id": ["ASSET_001", "ASSET_001"],
            "timestamp": ["2023-01-01T00:00:00", "2023-01-01T01:00:00"],
            "gross_generation_mwh": [51.0, 52.0],
            "net_generation_mwh": [50.0, 51.0],
            "curtailment_mwh": [0.5, 0.3],
            "availability_pct": [99.0, 99.0],
            "asset_capacity_mw": [100.0, 100.0],
        }
    )


@pytest.fixture
def temp_parquet_dir(tmp_path: Path) -> Path:
    """Directory for parquet files written during tests."""
    return tmp_path / "parquet"
