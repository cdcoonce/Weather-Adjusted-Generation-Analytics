"""Unit tests for ``weather_analytics.assets.analytics.dashboard_export``."""


import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import AssetKey, Failure, build_asset_context
from github.GithubException import GithubException

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
    waga_dashboard_export_publish,
)

# _ASSET_DIM_COLUMNS was removed in Phase 2 — asset dim is built by joining
# both marts.  Tests use the concrete mart column names instead.

_EXPECTED_NAN_ROWS = 2
_HTTP_FORBIDDEN = 403
_HTTP_NOT_FOUND = 404
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
            # Real mart column names (not the renamed data-contract names):
            "ASSET_CAPACITY_MW": [50.0 for _ in range(rows)],
            "ASSET_SIZE_CATEGORY": ["medium" for _ in range(rows)],
            "UNUSED_COLUMN": ["noise"] * rows,
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
    mock_resource, mock_conn = _make_mock_snowflake()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export.pl.read_database",
            side_effect=[daily_df, weather_df],
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
    assert result.metadata["schema_version"] == "1.0"

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

    # assets.json has display_name computed
    assets_records = json.loads((export_dir / _ASSETS_FILE).read_text())
    assert isinstance(assets_records, list)
    assert all("display_name" in r for r in assets_records)

    # manifest.json is valid JSON with expected keys
    manifest = json.loads((export_dir / _MANIFEST_FILE).read_text())
    assert "generated_at" in manifest
    assert "date_range" in manifest
    assert "asset_count" in manifest
    assert "row_counts" in manifest
    assert manifest["schema_version"] == "1.0"

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


# ===========================================================================
# waga_dashboard_export_publish
# ===========================================================================


def _make_mock_portfolio_repo() -> MagicMock:
    mock_repo_resource = MagicMock()
    mock_repo_resource.owner = "cdcoonce"
    mock_repo_resource.name = "charleslikesdata"
    mock_repo_resource.branch = "main"
    mock_repo_resource.token = "fake-pat"
    mock_repo_resource.full_name = "cdcoonce/charleslikesdata"
    return mock_repo_resource


def _make_mock_repo_with_git_trees() -> MagicMock:
    """Return a mock GitHub repo pre-configured for Git Trees API calls."""
    mock_repo = MagicMock()
    mock_ref = MagicMock()
    mock_ref.object.sha = "head-sha"
    mock_repo.get_git_ref.return_value = mock_ref
    mock_head_commit = MagicMock()
    mock_head_commit.tree = MagicMock()
    mock_repo.get_git_commit.return_value = mock_head_commit
    mock_blob = MagicMock()
    mock_blob.sha = "blob-sha"
    mock_repo.create_git_blob.return_value = mock_blob
    mock_repo.create_git_tree.return_value = MagicMock()
    mock_new_commit = MagicMock()
    mock_new_commit.sha = "new-commit-sha"
    mock_repo.create_git_commit.return_value = mock_new_commit
    return mock_repo


def _write_all_four_files(exports: Path) -> None:
    """Write the 4 expected export files to a tmp exports directory."""
    (exports / _DAILY_PERFORMANCE_FILE).write_text('[{"asset_id": "a"}]')
    (exports / _WEATHER_PERFORMANCE_FILE).write_text('[{"asset_id": "a"}]')
    (exports / _ASSETS_FILE).write_text('[{"asset_id": "a"}]')
    (exports / _MANIFEST_FILE).write_text('{"schema_version": "1.0"}')


@pytest.mark.unit
def test_publish_raises_failure_when_local_file_missing(
    tmp_path: Path,
) -> None:
    """If the build asset didn't run, publish must fail loudly."""
    empty_exports = tmp_path / "exports"
    empty_exports.mkdir()
    mock_repo_resource = _make_mock_portfolio_repo()
    context = build_asset_context()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            empty_exports,
        ),
        pytest.raises(Failure, match=r"(?i)missing"),
    ):
        waga_dashboard_export_publish(
            context=context, portfolio_repo=mock_repo_resource
        )


@pytest.mark.unit
def test_publish_single_commit_for_all_four_files(tmp_path: Path) -> None:
    """Git Trees API: one commit, 4 blobs, ref updated with new SHA."""
    exports = tmp_path / "exports"
    exports.mkdir()
    _write_all_four_files(exports)

    mock_repo_resource = _make_mock_portfolio_repo()
    mock_repo = _make_mock_repo_with_git_trees()
    mock_gh = MagicMock()
    mock_gh.get_repo.return_value = mock_repo

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            exports,
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export.Github",
            return_value=mock_gh,
            create=True,
        ),
    ):
        context = build_asset_context()
        result = waga_dashboard_export_publish(
            context=context, portfolio_repo=mock_repo_resource
        )

    # Single commit
    mock_repo.create_git_commit.assert_called_once()
    # one blob per file
    assert mock_repo.create_git_blob.call_count == _EXPECTED_FILE_COUNT
    # ref updated with the new commit SHA
    mock_repo.get_git_ref.return_value.edit.assert_called_once_with(
        sha="new-commit-sha"
    )
    assert result.metadata is not None
    assert result.metadata["commit_sha"] == "new-commit-sha"


@pytest.mark.unit
def test_publish_raises_failure_on_api_error(tmp_path: Path) -> None:
    """get_git_ref raising GithubException must raise Failure."""
    exports = tmp_path / "exports"
    exports.mkdir()
    _write_all_four_files(exports)

    mock_repo_resource = _make_mock_portfolio_repo()
    mock_repo = MagicMock()
    mock_repo.get_git_ref.side_effect = GithubException(status=500, data={})
    mock_gh = MagicMock()
    mock_gh.get_repo.return_value = mock_repo
    context = build_asset_context()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            exports,
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export.Github",
            return_value=mock_gh,
            create=True,
        ),
        pytest.raises(Failure, match="HTTP 500"),
    ):
        waga_dashboard_export_publish(
            context=context, portfolio_repo=mock_repo_resource
        )


@pytest.mark.unit
def test_publish_raises_failure_on_repo_not_found(tmp_path: Path) -> None:
    """If the PAT can't access the repo at all, fail with a clear message."""
    exports = tmp_path / "exports"
    exports.mkdir()
    _write_all_four_files(exports)

    mock_repo_resource = _make_mock_portfolio_repo()
    mock_gh = MagicMock()
    mock_gh.get_repo.side_effect = GithubException(status=_HTTP_NOT_FOUND, data={})
    context = build_asset_context()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            exports,
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export.Github",
            return_value=mock_gh,
            create=True,
        ),
        pytest.raises(Failure, match="Failed to access portfolio repo"),
    ):
        waga_dashboard_export_publish(
            context=context, portfolio_repo=mock_repo_resource
        )


@pytest.mark.unit
def test_publish_asset_key_and_group() -> None:
    asset_key = next(iter(waga_dashboard_export_publish.keys))
    assert asset_key == AssetKey(["waga_dashboard_export_publish"])
    assert (
        waga_dashboard_export_publish.group_names_by_key.get(asset_key) == "dashboard"
    )
