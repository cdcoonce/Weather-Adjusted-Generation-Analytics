"""Unit tests for the waga_generation_ingestion partitioned Dagster asset.

Validates asset metadata, concurrency tags, partitioning, empty-data guard,
and that the asset generates the expected number of records.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import (
    AssetKey,
    DailyPartitionsDefinition,
    Failure,
    build_asset_context,
)

from weather_analytics.assets.ingestion.generation import (
    waga_generation_ingestion,
)

# 24 hours * 10 assets
EXPECTED_DAILY_ROWS = 240


@pytest.mark.unit
def test_generation_ingestion_asset_has_concurrency_tag() -> None:
    """The asset should have the concurrency key op_tag."""
    node_def = waga_generation_ingestion.node_def
    assert node_def.tags == {
        "dagster/concurrency_key": "waga_ingestion",
    }


@pytest.mark.unit
def test_generation_ingestion_asset_group_name() -> None:
    """The asset should be in the waga_ingestion group."""
    assert (
        waga_generation_ingestion.group_names_by_key[
            AssetKey("waga_generation_ingestion")
        ]
        == "waga_ingestion"
    )


@pytest.mark.unit
def test_generation_ingestion_is_daily_partitioned() -> None:
    """The asset should have a DailyPartitionsDefinition."""
    partitions_def = waga_generation_ingestion.partitions_def
    assert isinstance(partitions_def, DailyPartitionsDefinition)


@pytest.mark.unit
def test_generation_ingestion_emits_expected_metadata() -> None:
    """The asset should emit partition_key, rows_generated, load_id,
    rows_loaded, and has_failed_jobs as output metadata."""
    fake_load_info = SimpleNamespace(
        loads_ids=["load_123"],
        has_failed_jobs=False,
        load_packages=[
            SimpleNamespace(
                schema_update={"generation": {"columns": {}}},
                jobs={"completed_jobs": []},
            )
        ],
    )
    fake_pipeline = MagicMock()
    fake_pipeline.run.return_value = fake_load_info

    fake_dlt_resource = MagicMock()
    fake_dlt_resource.create_pipeline.return_value = fake_pipeline

    context = build_asset_context(partition_key="2023-06-15")

    result = waga_generation_ingestion(
        context=context,
        dlt_ingestion=fake_dlt_resource,
    )

    assert "load_id" in result.metadata
    assert "partition_key" in result.metadata
    assert "rows_generated" in result.metadata
    assert "rows_loaded" in result.metadata
    assert "has_failed_jobs" in result.metadata
    assert result.metadata["partition_key"] == "2023-06-15"


@pytest.mark.unit
def test_generation_ingestion_generates_correct_row_count() -> None:
    """The dlt resource should receive 240 records (24h * 10 assets)."""
    fake_load_info = SimpleNamespace(
        loads_ids=["load_456"],
        has_failed_jobs=False,
        load_packages=[SimpleNamespace(schema_update={}, jobs={"completed_jobs": []})],
    )
    fake_pipeline = MagicMock()
    fake_pipeline.run.return_value = fake_load_info

    fake_dlt_resource = MagicMock()
    fake_dlt_resource.create_pipeline.return_value = fake_pipeline

    context = build_asset_context(partition_key="2023-06-15")

    result = waga_generation_ingestion(
        context=context,
        dlt_ingestion=fake_dlt_resource,
    )

    assert result.metadata["rows_generated"] == EXPECTED_DAILY_ROWS


@pytest.mark.unit
def test_generation_ingestion_calls_pipeline_run() -> None:
    """The asset should call pipeline.run with the dlt resource."""
    fake_load_info = SimpleNamespace(
        loads_ids=["load_789"],
        has_failed_jobs=False,
        load_packages=[SimpleNamespace(schema_update={}, jobs={"completed_jobs": []})],
    )
    fake_pipeline = MagicMock()
    fake_pipeline.run.return_value = fake_load_info

    fake_dlt_resource = MagicMock()
    fake_dlt_resource.create_pipeline.return_value = fake_pipeline

    context = build_asset_context(partition_key="2023-06-15")

    waga_generation_ingestion(
        context=context,
        dlt_ingestion=fake_dlt_resource,
    )

    fake_pipeline.run.assert_called_once()


@pytest.mark.unit
def test_generation_ingestion_raises_failure_on_empty_data() -> None:
    """The asset should raise dagster.Failure if the generator returns 0 rows."""

    with patch(
        "weather_analytics.assets.ingestion.generation.generate_generation_data",
        return_value=pl.DataFrame(),
    ):
        context = build_asset_context(partition_key="2023-06-15")
        fake_dlt_resource = MagicMock()

        with pytest.raises(Failure, match="0 rows"):
            waga_generation_ingestion(
                context=context,
                dlt_ingestion=fake_dlt_resource,
            )
