"""Unit tests for ``weather_analytics.assets.analytics.dashboard_export``."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import AssetKey, Failure, build_asset_context
from github.GithubException import GithubException

from weather_analytics.assets.analytics.dashboard_export import (
    _DAILY_PERFORMANCE_FILE,
    _EXPORT_DIR,
    _MIN_ROWS,
    _PHASE1_COLUMNS,
    _to_json_records,
    waga_dashboard_export_build,
    waga_dashboard_export_publish,
)

_EXPECTED_NAN_ROWS = 2
_HTTP_FORBIDDEN = 403
_HTTP_NOT_FOUND = 404


def _make_mock_snowflake() -> tuple[MagicMock, MagicMock]:
    mock_resource = MagicMock()
    mock_conn = MagicMock()
    mock_resource.get_connection.return_value = mock_conn
    return mock_resource, mock_conn


def _make_valid_mart_df(rows: int = _MIN_ROWS + 5) -> pl.DataFrame:
    """Return a mart-shaped DataFrame with uppercase columns like Snowflake."""
    return pl.DataFrame(
        {
            "ASSET_ID": [f"ASSET_{i:03d}" for i in range(rows)],
            "DATE": [f"2026-04-{(i % 28) + 1:02d}" for i in range(rows)],
            "TOTAL_NET_GENERATION_MWH": [100.0 + i for i in range(rows)],
            "DAILY_CAPACITY_FACTOR": [0.4 for _ in range(rows)],
            "UNUSED_COLUMN": ["noise"] * rows,
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
    """If the mart doesn't have the columns we need, fail loudly."""
    bad_df = pl.DataFrame(
        {
            "ASSET_ID": ["a"] * (_MIN_ROWS + 1),
            "DATE": ["2026-01-01"] * (_MIN_ROWS + 1),
            # Missing TOTAL_NET_GENERATION_MWH and DAILY_CAPACITY_FACTOR
        }
    )
    mock_resource, _ = _make_mock_snowflake()
    context = build_asset_context()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export.pl.read_database",
            return_value=bad_df,
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            tmp_path / "exports",
        ),
        pytest.raises(Failure, match="missing expected columns"),
    ):
        waga_dashboard_export_build(context=context, snowflake=mock_resource)


@pytest.mark.unit
def test_build_writes_json_and_lowercases_columns(tmp_path: Path) -> None:
    """Happy path: write JSON, emit metadata, lowercase columns, project."""
    export_dir = tmp_path / "exports"
    df = _make_valid_mart_df()
    mock_resource, mock_conn = _make_mock_snowflake()

    with (
        patch(
            "weather_analytics.assets.analytics.dashboard_export.pl.read_database",
            return_value=df,
        ),
        patch(
            "weather_analytics.assets.analytics.dashboard_export._EXPORT_DIR",
            export_dir,
        ),
    ):
        context = build_asset_context()
        result = waga_dashboard_export_build(context=context, snowflake=mock_resource)

    # Metadata
    assert result.metadata is not None
    assert result.metadata["row_count"] == df.shape[0]
    assert result.metadata["column_count"] == len(_PHASE1_COLUMNS)
    assert result.metadata["byte_size"] > 0
    assert result.metadata["schema_version"] == "1.0"

    # File was written
    output_path = export_dir / _DAILY_PERFORMANCE_FILE
    assert output_path.exists()

    # File is valid JSON, has correct lowercase columns, and is projected
    records = json.loads(output_path.read_text())
    assert isinstance(records, list)
    assert len(records) == df.shape[0]
    first = records[0]
    assert set(first.keys()) == set(_PHASE1_COLUMNS)  # projected
    assert "unused_column" not in first  # dropped
    assert "UNUSED_COLUMN" not in first  # case-normalized and dropped
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
        pytest.raises(Failure, match="missing"),
    ):
        waga_dashboard_export_publish(
            context=context, portfolio_repo=mock_repo_resource
        )


@pytest.mark.unit
def test_publish_creates_new_file_when_not_present(tmp_path: Path) -> None:
    """When remote file doesn't exist yet, use create_file (not update)."""
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / _DAILY_PERFORMANCE_FILE).write_text('[{"asset_id": "a"}]')

    mock_repo_resource = _make_mock_portfolio_repo()

    mock_repo = MagicMock()
    mock_repo.get_contents.side_effect = GithubException(
        status=_HTTP_NOT_FOUND, data={}
    )
    mock_repo.create_file.return_value = {
        "content": MagicMock(sha="abc123"),
        "commit": MagicMock(sha="deadbeef"),
    }
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

    assert mock_repo.create_file.called
    assert not mock_repo.update_file.called
    assert result.metadata is not None
    assert result.metadata["commit_sha"] == "deadbeef"


@pytest.mark.unit
def test_publish_updates_existing_file(tmp_path: Path) -> None:
    """When remote file exists, use update_file with its SHA."""
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / _DAILY_PERFORMANCE_FILE).write_text('[{"asset_id": "a"}]')

    mock_repo_resource = _make_mock_portfolio_repo()

    mock_existing = MagicMock()
    mock_existing.sha = "existing-sha"
    mock_repo = MagicMock()
    mock_repo.get_contents.return_value = mock_existing
    mock_repo.update_file.return_value = {
        "content": MagicMock(sha="new-sha"),
        "commit": MagicMock(sha="deadbeef2"),
    }
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

    mock_repo.update_file.assert_called_once()
    update_kwargs = mock_repo.update_file.call_args.kwargs
    assert update_kwargs["sha"] == "existing-sha"
    assert result.metadata["commit_sha"] == "deadbeef2"


@pytest.mark.unit
def test_publish_raises_failure_on_api_error(tmp_path: Path) -> None:
    """Non-404 GitHub errors must raise Failure (no silent success)."""
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / _DAILY_PERFORMANCE_FILE).write_text('[{"asset_id": "a"}]')

    mock_repo_resource = _make_mock_portfolio_repo()

    mock_repo = MagicMock()
    mock_repo.get_contents.side_effect = GithubException(
        status=_HTTP_FORBIDDEN, data={}
    )
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
        pytest.raises(Failure, match="HTTP 403"),
    ):
        waga_dashboard_export_publish(
            context=context, portfolio_repo=mock_repo_resource
        )


@pytest.mark.unit
def test_publish_raises_failure_on_repo_not_found() -> None:
    """If the PAT can't access the repo at all, fail with a clear message."""
    # Use the actual _EXPORT_DIR so we reach the auth step, but ensure the
    # file exists so we don't short-circuit on the missing-file guard.
    _EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    (_EXPORT_DIR / _DAILY_PERFORMANCE_FILE).write_text("[]")

    mock_repo_resource = _make_mock_portfolio_repo()
    mock_gh = MagicMock()
    mock_gh.get_repo.side_effect = GithubException(status=_HTTP_NOT_FOUND, data={})
    context = build_asset_context()

    with (
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
