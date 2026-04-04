"""Unit tests for the waga_weather_ingestion Dagster asset.

Validates asset metadata, concurrency tags, and that the asset function
emits the expected output metadata keys.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest
from dagster import (
    AssetKey,
    build_asset_context,
)

from weather_analytics.assets.ingestion.weather import (
    WeatherIngestionConfig,
    waga_weather_ingestion,
)


@pytest.mark.unit
def test_weather_ingestion_asset_has_concurrency_tag() -> None:
    """The asset should have the concurrency key op_tag to prevent
    concurrent merges."""
    # Access op tags from the underlying node definition
    node_def = waga_weather_ingestion.node_def
    assert node_def.tags == {
        "dagster/concurrency_key": "waga_ingestion",
    }


@pytest.mark.unit
def test_weather_ingestion_asset_group_name() -> None:
    """The asset should be in the waga_ingestion group."""
    assert (
        waga_weather_ingestion.group_names_by_key[AssetKey("waga_weather_ingestion")]
        == "waga_ingestion"
    )


@pytest.mark.unit
def test_weather_ingestion_emits_expected_metadata(
    tmp_path: Path,
) -> None:
    """The asset should emit load_id, rows_loaded, and has_failed_jobs
    as output metadata."""
    # Create a small parquet file in tmp_path
    df = pl.DataFrame(
        {
            "asset_id": ["ASSET_001", "ASSET_001"],
            "timestamp": [
                "2023-01-01T00:00:00",
                "2023-01-01T01:00:00",
            ],
            "wind_speed_mps": [5.0, 6.0],
            "wind_direction_deg": [180.0, 190.0],
            "ghi": [0.0, 0.0],
            "temperature_c": [10.0, 11.0],
            "pressure_hpa": [1013.0, 1012.0],
            "relative_humidity": [65.0, 64.0],
        }
    )
    parquet_dir = tmp_path / "weather"
    parquet_dir.mkdir()
    df.write_parquet(parquet_dir / "weather_2023-01-01.parquet")

    # Mock the dlt resource to avoid real Snowflake calls
    fake_load_info = SimpleNamespace(
        loads_ids=["load_123"],
        has_failed_jobs=False,
        load_packages=[
            SimpleNamespace(
                schema_update={"weather": {"columns": {}}},
                jobs={"completed_jobs": []},
            )
        ],
    )
    fake_pipeline = MagicMock()
    fake_pipeline.run.return_value = fake_load_info

    fake_dlt_resource = MagicMock()
    fake_dlt_resource.create_pipeline.return_value = fake_pipeline

    context = build_asset_context()

    result = waga_weather_ingestion(
        context=context,
        config=WeatherIngestionConfig(source_path=str(parquet_dir)),
        dlt_ingestion=fake_dlt_resource,
    )

    # Verify metadata keys
    assert "load_id" in result.metadata
    assert "rows_loaded" in result.metadata
    assert "has_failed_jobs" in result.metadata


@pytest.mark.unit
def test_weather_ingestion_calls_pipeline_run_with_merge(
    tmp_path: Path,
) -> None:
    """The asset should call pipeline.run with the weather dlt resource."""
    parquet_dir = tmp_path / "weather"
    parquet_dir.mkdir()
    df = pl.DataFrame(
        {
            "asset_id": ["ASSET_001"],
            "timestamp": ["2023-01-01T00:00:00"],
            "wind_speed_mps": [5.0],
            "wind_direction_deg": [180.0],
            "ghi": [0.0],
            "temperature_c": [10.0],
            "pressure_hpa": [1013.0],
            "relative_humidity": [65.0],
        }
    )
    df.write_parquet(parquet_dir / "weather_2023-01-01.parquet")

    fake_load_info = SimpleNamespace(
        loads_ids=["load_456"],
        has_failed_jobs=False,
        load_packages=[SimpleNamespace(schema_update={}, jobs={"completed_jobs": []})],
    )
    fake_pipeline = MagicMock()
    fake_pipeline.run.return_value = fake_load_info

    fake_dlt_resource = MagicMock()
    fake_dlt_resource.create_pipeline.return_value = fake_pipeline

    context = build_asset_context()

    waga_weather_ingestion(
        context=context,
        config=WeatherIngestionConfig(source_path=str(parquet_dir)),
        dlt_ingestion=fake_dlt_resource,
    )

    # Verify pipeline.run was called
    fake_pipeline.run.assert_called_once()
