"""Unit tests for `weather_adjusted_generation_analytics.mock_data.generate_weather`.

These tests validate the full weather data generator output shape and value bounds.
They avoid any file IO.

"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from weather_adjusted_generation_analytics.mock_data.generate_weather import (
    generate_weather_data,
)


@pytest.mark.unit
def test_generate_weather_data_output_shape_and_value_bounds() -> None:
    """Validate shape, schema, and basic bounds for generated weather data.

    This is intentionally a lightweight contract test:
    - verifies expected columns exist
    - verifies row count equals (hours * asset_count)
    - verifies known value bounds from the implementation

    """
    start_date = "2023-01-01T00:00:00"
    end_date = "2023-01-01T03:00:00"  # inclusive hourly range => 4 hours
    asset_count = 3

    df = generate_weather_data(
        start_date=start_date,
        end_date=end_date,
        asset_count=asset_count,
        random_seed=123,
    )

    expected_columns = {
        "timestamp",
        "asset_id",
        "wind_speed_mps",
        "wind_direction_deg",
        "ghi",
        "temperature_c",
        "pressure_hpa",
        "relative_humidity",
    }
    assert set(df.columns) == expected_columns

    # Shape expectations
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)
    expected_hours = int(((end_dt - start_dt).total_seconds() // 3600) + 1)
    assert df.shape[0] == expected_hours * asset_count
    assert df.select(pl.col("asset_id").n_unique()).item() == asset_count

    # Bounds encoded in the generator
    assert df.select(pl.col("wind_speed_mps").min()).item() >= 0.0
    assert df.select(pl.col("wind_speed_mps").max()).item() <= 25.0

    assert df.select(pl.col("wind_direction_deg").min()).item() >= 0.0
    assert df.select(pl.col("wind_direction_deg").max()).item() < 360.0

    assert df.select(pl.col("ghi").min()).item() >= 0.0
    assert df.select(pl.col("ghi").max()).item() <= 1000.0

    assert df.select(pl.col("relative_humidity").min()).item() >= 20.0
    assert df.select(pl.col("relative_humidity").max()).item() <= 95.0
