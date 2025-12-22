"""Unit tests for `weather_adjusted_generation_analytics.utils.polars_utils`.

These tests focus on deterministic transforms and edge cases.

"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from weather_adjusted_generation_analytics.utils.polars_utils import (
    add_lag_features,
    add_lead_features,
    add_rolling_stats,
    add_time_features,
    calculate_capacity_factor,
    calculate_correlation,
    filter_by_date_range,
)


@pytest.mark.unit
def test_add_lag_features_adds_expected_columns_no_partition() -> None:
    df = pl.DataFrame({"x": [1, 2, 3, 4]})
    out = add_lag_features(df, column="x", lags=[1, 2])

    assert set(out.columns) == {"x", "x_lag_1", "x_lag_2"}
    assert out["x_lag_1"].to_list() == [None, 1, 2, 3]
    assert out["x_lag_2"].to_list() == [None, None, 1, 2]


@pytest.mark.unit
def test_add_lag_features_partition_does_not_bleed_between_groups() -> None:
    df = pl.DataFrame(
        {
            "asset_id": ["a", "a", "b", "b"],
            "x": [1, 2, 10, 11],
        }
    )

    out = add_lag_features(df, column="x", lags=[1], partition_by="asset_id")
    assert out["x_lag_1"].to_list() == [None, 1, None, 10]


@pytest.mark.unit
def test_add_lead_features_adds_expected_columns_no_partition() -> None:
    df = pl.DataFrame({"x": [1, 2, 3, 4]})
    out = add_lead_features(df, column="x", leads=[1, 2])

    assert set(out.columns) == {"x", "x_lead_1", "x_lead_2"}
    assert out["x_lead_1"].to_list() == [2, 3, 4, None]
    assert out["x_lead_2"].to_list() == [3, 4, None, None]


@pytest.mark.unit
def test_add_rolling_stats_default_stats_creates_columns_and_values() -> None:
    df = pl.DataFrame({"x": [1, 2, 3, 4]})
    out = add_rolling_stats(df, column="x", window_sizes=[2])

    assert "x_rolling_mean_2" in out.columns
    assert "x_rolling_std_2" in out.columns

    # For window=2 rolling mean: [null, 1.5, 2.5, 3.5]
    mean_vals = out["x_rolling_mean_2"].to_list()
    assert mean_vals[0] is None
    assert mean_vals[1:] == [1.5, 2.5, 3.5]


@pytest.mark.unit
def test_add_rolling_stats_skips_unknown_stats() -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    out = add_rolling_stats(df, column="x", window_sizes=[2], stats=["mean", "bogus"])

    assert "x_rolling_mean_2" in out.columns
    assert "x_rolling_bogus_2" not in out.columns


@pytest.mark.unit
def test_calculate_correlation_static_no_partition_returns_single_row_df() -> None:
    df = pl.DataFrame({"a": [1.0, 2.0, 3.0], "b": [1.0, 2.0, 3.0]})
    out = calculate_correlation(df, col1="a", col2="b")

    assert out.shape == (1, 1)
    assert out.columns == ["corr_a_b"]
    assert out["corr_a_b"].item() == pytest.approx(1.0)


@pytest.mark.unit
def test_calculate_correlation_static_partition_returns_grouped_rows() -> None:
    df = pl.DataFrame(
        {
            "asset_id": ["a", "a", "b", "b"],
            "x": [1.0, 2.0, 1.0, 2.0],
            "y": [1.0, 2.0, 2.0, 1.0],
        }
    )

    out = calculate_correlation(df, col1="x", col2="y", partition_by="asset_id")

    assert set(out.columns) == {"asset_id", "corr_x_y"}
    assert out.shape[0] == 2


@pytest.mark.unit
def test_add_time_features_extracts_expected_fields() -> None:
    df = pl.DataFrame({"timestamp": [datetime(2023, 1, 1, 13, 0, 0)]}).with_columns(
        pl.col("timestamp").cast(pl.Datetime)
    )

    out = add_time_features(df)

    assert out["hour"].item() == 13
    assert out["day"].item() == 1
    assert out["month"].item() == 1
    assert out["quarter"].item() == 1
    assert out["year"].item() == 2023


@pytest.mark.unit
def test_calculate_capacity_factor_adds_column_and_uses_hours() -> None:
    df = pl.DataFrame({"gen": [50.0], "cap": [100.0]})
    out = calculate_capacity_factor(df, generation_col="gen", capacity_col="cap", hours=2.0)

    assert "capacity_factor" in out.columns
    assert out["capacity_factor"].item() == pytest.approx(50.0 / (100.0 * 2.0))


@pytest.mark.unit
def test_filter_by_date_range_inclusive_for_string_timestamps() -> None:
    df = pl.DataFrame(
        {
            "timestamp": [
                "2023-01-01",
                "2023-01-02",
                "2023-01-03",
            ],
            "x": [1, 2, 3],
        }
    )

    out = filter_by_date_range(df, start_date="2023-01-02", end_date="2023-01-03")
    assert out["x"].to_list() == [2, 3]
