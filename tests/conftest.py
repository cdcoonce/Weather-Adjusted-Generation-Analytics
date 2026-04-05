"""Pytest configuration and shared fixtures."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


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
