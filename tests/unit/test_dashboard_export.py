"""Unit tests for ``weather_analytics.assets.analytics.dashboard_export``."""


import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import AssetKey, Failure, build_asset_context

from weather_analytics.assets.analytics.dashboard_export import (
    _ASSETS_FILE,
    _DAILY_PERFORMANCE_COLUMNS,
    _DAILY_PERFORMANCE_FILE,
    _MANIFEST_FILE,
    _MIN_ROWS,
    _WEATHER_PERFORMANCE_COLUMNS,
    _WEATHER_PERFORMANCE_FILE,
    _to_json_records,
    waga_dashboard_export_build,
)

# _ASSET_DIM_COLUMNS was removed in Phase 2 — asset dim is built by joining
# both marts.  Tests use the concrete mart column names instead.

_EXPECTED_NAN_ROWS = 2
_EXPECTED_FILE_COUNT = 4


def _make_mock_snowflake() -> tuple[MagicMock, MagicMock]:
    mock_resource = MagicMock()
    mock_conn = MagicMock()
    mock_resource.get_connection.return_value = mock_conn
    return mock_resource, mock_conn


def _make_valid_mart_df(rows: int = _MIN_ROWS + 5) -> pl.DataFrame:
    """Return a daily-mart-shaped DataFrame with uppercase columns like Snowflake.

    Column names match the real ``mart_asset_performance_daily`` schema:
    ``ASSET_CAPACITY_MW`` and ``ASSET_SIZE_CATEGORY`` (not the shorter names
    that the data contract exports use after renaming).
    """
    return pl.DataFrame(
        {
            "ASSET_ID": [f"ASSET_{i:03d}" for i in range(rows)],
            "DATE": [f"2026-04-{(i % 28) + 1:02d}" for i in range(rows)],
            "ASSET_TYPE": ["wind" for _ in range(rows)],
            "TOTAL_NET_GENERATION_MWH": [100.0 + i for i in range(rows)],
            "DAILY_CAPACITY_FACTOR": [0.4 for _ in range(rows)],
            "AVG_AVAILABILITY_PCT": [98.0 for _ in range(rows)],
            "TOTAL_CURTAILMENT_MWH": [0.0 for _ in range(rows)],
            "DAILY_PERFORMANCE_RATING": ["good" for _ in range(rows)],
            "EXCELLENT_HOURS": [4 for _ in range(rows)],
            "GOOD_HOURS": [10 for _ in range(rows)],
            "FAIR_HOURS": [6 for _ in range(rows)],
            "POOR_HOURS": [4 for _ in range(rows)],
            "AVG_WIND_SPEED_MPS": [7.2 for _ in range(rows)],
            "AVG_GHI": [450.0 for _ in range(rows)],
            "AVG_TEMPERATURE_C": [18.5 for _ in range(rows)],
            "DATA_COMPLETENESS_PCT": [100.0 for _ in range(rows)],
            # Technology-specific columns (null for wind in this fixture):
            "AVG_SOC_PCT": [None for _ in range(rows)],
            "TOTAL_CHARGE_MWH": [None for _ in range(rows)],
            "TOTAL_DISCHARGE_MWH": [None for _ in range(rows)],
            "TOTAL_FUEL_MMBTU": [None for _ in range(rows)],
            "AVG_HEAT_RATE_BTU_KWH": [None for _ in range(rows)],
            "TOTAL_CO2_TONNES": [None for _ in range(rows)],
            # Real mart column names (not the renamed data-contract names):
            "ASSET_CAPACITY_MW": [50.0 for _ in range(rows)],
            "ASSET_SIZE_CATEGORY": ["medium" for _ in range(rows)],
            "UNUSED_COLUMN": ["noise"] * rows,
        }
    )


def _make_valid_dim_df(rows: int = _MIN_ROWS + 5) -> pl.DataFrame:
    """Return a dim_asset-shaped DataFrame with uppercase columns like Snowflake."""
    return pl.DataFrame(
        {
            "ASSET_ID": [f"ASSET_{i:03d}" for i in range(rows)],
            "ASSET_NAME": [f"Site {i:03d}" for i in range(rows)],
            "ASSET_TYPE": ["wind" for _ in range(rows)],
            "ASSET_CAPACITY_MW": [50.0 for _ in range(rows)],
            "ASSET_SIZE_CATEGORY": ["Medium" for _ in range(rows)],
            "LATITUDE": [35.0 for _ in range(rows)],
            "LONGITUDE": [-100.0 for _ in range(rows)],
            "REGION": ["ERCOT" for _ in range(rows)],
        }
    )


def _make_valid_weather_mart_df(rows: int = _MIN_ROWS + 5) -> pl.DataFrame:
    """Return a weather-mart-shaped DataFrame with uppercase columns like Snowflake."""
    return pl.DataFrame(
        {
            "ASSET_ID": [f"ASSET_{i:03d}" for i in range(rows)],
            "DATE": [f"2026-04-{(i % 28) + 1:02d}" for i in range(rows)],
            "PERFORMANCE_SCORE": [0.85 for _ in range(rows)],
            "PERFORMANCE_CATEGORY": ["good" for _ in range(rows)],
            "AVG_EXPECTED_GENERATION_MWH": [100.0 for _ in range(rows)],
            "AVG_ACTUAL_GENERATION_MWH": [85.0 for _ in range(rows)],
            "AVG_PERFORMANCE_RATIO_PCT": [85.0 for _ in range(rows)],
            "WIND_R_SQUARED": [0.92 for _ in range(rows)],
            "SOLAR_R_SQUARED": [0.0 for _ in range(rows)],
            "INFERRED_ASSET_TYPE": ["wind" for _ in range(rows)],
            "ROLLING_7D_AVG_CF": [0.40 for _ in range(rows)],
            "ROLLING_30D_AVG_CF": [0.38 for _ in range(rows)],
        }
    )


# ===========================================================================
# waga_dashboard_export_build
# ===========================================================================


@pytest.mark.unit
def test_build_raises_failure_on_insufficient_rows(tmp_path: Path) -> None:
    """Empty mart guard — matches correlation.py pattern, CEO review item."""
    small_df = pl.DataFrame(
        {
            "ASSET_ID": ["a"] * 5,
            "DATE": ["2026-01-01"] * 5,
            "TOTAL_NET_GENERATION_MWH": [1.0] * 5,
            "DAILY_CAPACITY_FACTOR": [0.4] * 5,
        }
    )
    mock_resource, _ = _make_mock_snowflake()
    context = build_asset_context()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export.pl.read_database",
            return_value=small_df,
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            tmp_path / "exports",
        ),
        pytest.raises(Failure, match="need at least"),
    ):
        waga_dashboard_export_build(context=context, snowflake=mock_resource)


@pytest.mark.unit
def test_build_raises_failure_on_missing_column(tmp_path: Path) -> None:
    """If the daily mart doesn't have the columns we need, fail loudly."""
    bad_df = pl.DataFrame(
        {
            "ASSET_ID": ["a"] * (_MIN_ROWS + 1),
            "DATE": ["2026-01-01"] * (_MIN_ROWS + 1),
            # Missing required columns
        }
    )
    weather_df = _make_valid_weather_mart_df()
    mock_resource, _ = _make_mock_snowflake()
    context = build_asset_context()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export.pl.read_database",
            side_effect=[bad_df, weather_df],
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            tmp_path / "exports",
        ),
        pytest.raises(Failure, match="missing expected columns"),
    ):
        waga_dashboard_export_build(context=context, snowflake=mock_resource)


@pytest.mark.unit
def test_build_raises_failure_if_weather_mart_empty(tmp_path: Path) -> None:
    """If the weather mart has too few rows, fail with Failure."""
    daily_df = _make_valid_mart_df()
    small_weather_df = _make_valid_weather_mart_df(rows=3)
    mock_resource, _ = _make_mock_snowflake()
    context = build_asset_context()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export.pl.read_database",
            side_effect=[daily_df, small_weather_df],
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            tmp_path / "exports",
        ),
        pytest.raises(Failure, match="need at least"),
    ):
        waga_dashboard_export_build(context=context, snowflake=mock_resource)


@pytest.mark.unit
def test_build_writes_all_four_files(tmp_path: Path) -> None:
    """Happy path: write 4 JSON files, emit metadata, lowercase columns, project."""
    export_dir = tmp_path / "exports"
    daily_df = _make_valid_mart_df()
    weather_df = _make_valid_weather_mart_df()
    dim_df = _make_valid_dim_df()
    mock_resource, mock_conn = _make_mock_snowflake()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export.pl.read_database",
            side_effect=[daily_df, weather_df, dim_df],
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            export_dir,
        ),
    ):
        context = build_asset_context()
        result = waga_dashboard_export_build(context=context, snowflake=mock_resource)

    # Metadata has all 4 file sizes
    assert result.metadata is not None
    assert result.metadata["daily_performance_bytes"] > 0
    assert result.metadata["weather_performance_bytes"] > 0
    assert result.metadata["assets_bytes"] > 0
    assert result.metadata["manifest_bytes"] > 0
    assert result.metadata["schema_version"] == "2.0"

    # All 4 files were written
    assert (export_dir / _DAILY_PERFORMANCE_FILE).exists()
    assert (export_dir / _WEATHER_PERFORMANCE_FILE).exists()
    assert (export_dir / _ASSETS_FILE).exists()
    assert (export_dir / _MANIFEST_FILE).exists()

    # daily_performance.json has correct lowercase columns, projected
    daily_records = json.loads((export_dir / _DAILY_PERFORMANCE_FILE).read_text())
    assert isinstance(daily_records, list)
    assert len(daily_records) == daily_df.shape[0]
    first_daily = daily_records[0]
    assert set(first_daily.keys()) == set(_DAILY_PERFORMANCE_COLUMNS)
    assert "unused_column" not in first_daily

    # weather_performance.json has correct lowercase columns
    weather_records = json.loads((export_dir / _WEATHER_PERFORMANCE_FILE).read_text())
    assert isinstance(weather_records, list)
    first_weather = weather_records[0]
    assert set(first_weather.keys()) == set(_WEATHER_PERFORMANCE_COLUMNS)

    # assets.json has display_name + real dimension fields from dim_asset
    assets_records = json.loads((export_dir / _ASSETS_FILE).read_text())
    assert isinstance(assets_records, list)
    assert all("display_name" in r for r in assets_records)
    first_asset = assets_records[0]
    for field in ("name", "region", "latitude", "longitude", "capacity_mw"):
        assert field in first_asset
    assert first_asset["name"] in first_asset["display_name"]

    # manifest.json is valid JSON with expected keys
    manifest = json.loads((export_dir / _MANIFEST_FILE).read_text())
    assert "generated_at" in manifest
    assert "date_range" in manifest
    assert "asset_count" in manifest
    assert "row_counts" in manifest
    assert manifest["schema_version"] == "2.0"

    # Connection closed
    mock_conn.close.assert_called_once()


@pytest.mark.unit
def test_build_asset_key_and_group() -> None:
    asset_key = next(iter(waga_dashboard_export_build.keys))
    assert asset_key == AssetKey(["waga_dashboard_export_build"])
    assert waga_dashboard_export_build.group_names_by_key.get(asset_key) == "dashboard"


@pytest.mark.unit
def test_to_json_records_handles_dates_and_nan() -> None:
    """Date columns must serialize as strings; NaN must become None."""
    df = pl.DataFrame(
        {
            "asset_id": ["a", "b"],
            "date": [date(2026, 4, 10), date(2026, 4, 11)],
            "total_net_generation_mwh": [100.0, float("nan")],
            "daily_capacity_factor": [0.5, 0.4],
        }
    )
    records = _to_json_records(df)
    payload = json.dumps(records)  # must be JSON-serializable
    reloaded = json.loads(payload)
    assert len(reloaded) == _EXPECTED_NAN_ROWS
    assert reloaded[0]["date"] == "2026-04-10"
    # NaN becomes None -> null in JSON
    assert reloaded[1]["total_net_generation_mwh"] is None
