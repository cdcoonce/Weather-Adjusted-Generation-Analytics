"""Unit tests for ``weather_analytics.assets.analytics.correlation``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import AssetKey, Failure, build_asset_context

from weather_analytics.assets.analytics.correlation import (
    _MIN_ROWS,
    waga_correlation_analysis,
)


def _make_mock_snowflake() -> tuple[MagicMock, MagicMock]:
    """Return a (mock_resource, mock_conn) pair."""
    mock_resource = MagicMock()
    mock_conn = MagicMock()
    mock_resource.get_connection.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_resource, mock_conn


@pytest.mark.unit
def test_raises_failure_on_insufficient_rows() -> None:
    """Empty mart guard -- CEO review finding #3."""
    small_df = pl.DataFrame(
        {
            "asset_id": ["a"] * 5,
            "total_net_generation_mwh": [1.0] * 5,
            "avg_temperature_c": [20.0] * 5,
        }
    )
    mock_resource, _ = _make_mock_snowflake()

    with (
        patch(
            "weather_analytics.assets.analytics.correlation.pl.read_database",
            return_value=small_df,
        ),
        pytest.raises(Failure, match="need at least"),
    ):
        context = build_asset_context()
        waga_correlation_analysis(context=context, snowflake=mock_resource)


@pytest.mark.unit
def test_emits_metadata_on_success() -> None:
    n = _MIN_ROWS + 5
    df = pl.DataFrame(
        {
            "asset_id": ["a"] * n,
            "total_net_generation_mwh": list(range(n)),
            "avg_temperature_c": list(range(n)),
        }
    )
    mock_resource, mock_conn = _make_mock_snowflake()

    with patch(
        "weather_analytics.assets.analytics.correlation.pl.read_database",
        return_value=df,
    ):
        context = build_asset_context()
        result = waga_correlation_analysis(context=context, snowflake=mock_resource)

    assert result.metadata is not None
    assert "row_count" in result.metadata
    assert "enriched_row_count" in result.metadata
    assert "mean_correlation" in result.metadata


@pytest.mark.unit
def test_calls_get_connection() -> None:
    n = _MIN_ROWS + 5
    df = pl.DataFrame(
        {
            "asset_id": ["a"] * n,
            "total_net_generation_mwh": list(range(n)),
            "avg_temperature_c": list(range(n)),
        }
    )
    mock_resource, mock_conn = _make_mock_snowflake()

    with patch(
        "weather_analytics.assets.analytics.correlation.pl.read_database",
        return_value=df,
    ):
        context = build_asset_context()
        waga_correlation_analysis(context=context, snowflake=mock_resource)

    mock_resource.get_connection.assert_called_once()
    mock_conn.close.assert_called_once()


@pytest.mark.unit
def test_asset_key_and_group() -> None:
    """Verify the asset decorator properties."""
    asset_key = next(iter(waga_correlation_analysis.keys))
    assert asset_key == AssetKey(["waga_correlation_analysis"])
    assert (
        waga_correlation_analysis.group_names_by_key.get(asset_key) == "waga_analytics"
    )
