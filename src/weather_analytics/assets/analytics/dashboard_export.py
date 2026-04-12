"""Dashboard export assets: build JSON from marts and publish to portfolio repo.

Two assets are defined here, split for clean separation of concerns:

- ``waga_dashboard_export_build`` queries Snowflake marts, lowercases column
  names (matching ``correlation.py:60-61``), projects to the UI subset, and
  writes JSON files to a local directory.
- ``waga_dashboard_export_publish`` reads the local JSON files and pushes
  them to the portfolio repo via the GitHub Contents API. Retries twice
  with a 60-second delay.

Phase 1 builds a minimal tracer bullet: one JSON file with a small column
subset. Phase 2 expands the projection to the full 15-column subset and
adds the other three JSON files.

PyGithub debug logging is never enabled — that would leak the PAT in
request headers. Do not change this.
"""

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from github import Github
from github.GithubException import GithubException

from weather_analytics.resources.portfolio_repo import PortfolioRepoResource
from weather_analytics.resources.snowflake import WAGASnowflakeResource


def _refresh_timestamp() -> str:
    """Return an ISO 8601 UTC timestamp suffix for commit messages.

    Using a timestamp rather than ``context.run.run_id`` avoids a Dagster
    testing quirk: ``context.run.run_id`` is unavailable when an asset is
    invoked directly via ``build_asset_context()``.
    """
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MIN_ROWS = 10
_SOURCE_MART = "WAGA.MARTS.mart_asset_performance_daily"
_EXPORT_DIR = Path("dashboard_exports")
_SCHEMA_VERSION = "1.0"
_HTTP_NOT_FOUND = 404

# Phase 1 uses a minimal column subset as the tracer bullet. Phase 2 expands
# to the full 15-column contract specified in the design spec.
_PHASE1_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "date",
    "total_net_generation_mwh",
    "daily_capacity_factor",
)

_DAILY_PERFORMANCE_FILE = "daily_performance.json"
_REPO_DATA_DIR = "dashboard/data"


# ===========================================================================
# Build asset: query marts, write local JSON
# ===========================================================================


@asset(
    name="waga_dashboard_export_build",
    group_name="dashboard",
    deps=["mart_asset_performance_daily"],
)
def waga_dashboard_export_build(
    context: AssetExecutionContext,
    snowflake: WAGASnowflakeResource,
) -> MaterializeResult:
    """Query mart, project columns, write JSON files locally.

    Downstream assets (``waga_dashboard_export_publish``) read the local
    files and push them to the portfolio repo.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context.
    snowflake : WAGASnowflakeResource
        Authenticated Snowflake resource.

    Returns
    -------
    MaterializeResult
        Metadata including row count, byte sizes, and output paths.

    Raises
    ------
    dagster.Failure
        If the source mart has fewer than ``_MIN_ROWS`` rows. Prevents
        publishing an empty-looking dashboard (matches
        ``correlation.py:63-69`` pattern).
    """
    conn = snowflake.get_connection()
    try:
        query_start = time.monotonic()
        raw_df = pl.read_database(
            query=f"SELECT * FROM {_SOURCE_MART}",
            connection=conn,
        )
        # Snowflake returns UPPERCASE columns; normalize to match dbt
        # model definitions (see correlation.py:60-61).
        raw_df = raw_df.rename({col: col.lower() for col in raw_df.columns})
        query_ms = (time.monotonic() - query_start) * 1000
        context.log.info(
            "Snowflake query completed in %.0fms (%d rows)",
            query_ms,
            raw_df.shape[0],
        )

        if raw_df.shape[0] < _MIN_ROWS:
            raise Failure(
                description=(
                    f"Source mart {_SOURCE_MART} has {raw_df.shape[0]} rows, "
                    f"need at least {_MIN_ROWS}. Refusing to publish a "
                    f"broken-looking dashboard."
                ),
            )

        # Project to the Phase 1 column subset.
        project_start = time.monotonic()
        missing = [c for c in _PHASE1_COLUMNS if c not in raw_df.columns]
        if missing:
            raise Failure(
                description=(
                    f"Mart {_SOURCE_MART} is missing expected columns: "
                    f"{missing}. Check dbt model schema."
                ),
            )
        projected = raw_df.select(_PHASE1_COLUMNS)
        project_ms = (time.monotonic() - project_start) * 1000
        context.log.info("Column projection completed in %.0fms", project_ms)

        # Serialize to JSON-friendly records.
        serialize_start = time.monotonic()
        records = _to_json_records(projected)
        payload = json.dumps(records, separators=(",", ":"))
        serialize_ms = (time.monotonic() - serialize_start) * 1000
        context.log.info(
            "JSON serialization completed in %.0fms (%d bytes)",
            serialize_ms,
            len(payload),
        )

        # Write to local dashboard_exports/.
        write_start = time.monotonic()
        _EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = _EXPORT_DIR / _DAILY_PERFORMANCE_FILE
        output_path.write_text(payload, encoding="utf-8")
        write_ms = (time.monotonic() - write_start) * 1000
        context.log.info(
            "Local JSON write completed in %.0fms (%s)",
            write_ms,
            output_path,
        )

        return MaterializeResult(
            metadata={
                "row_count": projected.shape[0],
                "column_count": projected.shape[1],
                "byte_size": len(payload),
                "output_path": MetadataValue.path(str(output_path)),
                "schema_version": _SCHEMA_VERSION,
            },
        )
    finally:
        conn.close()


def _to_json_records(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert a Polars DataFrame to JSON-serializable records.

    Dates are serialized as ISO strings; NaN is replaced with None.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to serialize.

    Returns
    -------
    list[dict[str, Any]]
        JSON-ready records.
    """
    # Replace NaN -> None and stringify dates via Polars built-in.
    cleaned = df.fill_nan(None)
    # Polars' to_dicts handles dates correctly when we coerce them first.
    string_cols = [
        pl.col(c).cast(pl.Utf8) if cleaned[c].dtype == pl.Date else pl.col(c)
        for c in cleaned.columns
    ]
    return cleaned.select(string_cols).to_dicts()


# ===========================================================================
# Publish asset: read local JSON, push to portfolio repo via Contents API
# ===========================================================================


@asset(
    name="waga_dashboard_export_publish",
    group_name="dashboard",
    deps=["waga_dashboard_export_build"],
    retry_policy=RetryPolicy(max_retries=2, delay=60),
)
def waga_dashboard_export_publish(
    context: AssetExecutionContext,
    portfolio_repo: PortfolioRepoResource,
) -> MaterializeResult:
    """Read local JSON files and push them to the portfolio repo.

    Uses the GitHub Contents API via PyGithub. This works from Dagster
    Cloud serverless because it's a plain HTTPS request — no git clone
    or SSH required.

    Idempotent: the Contents API replaces files by SHA, so running twice
    produces either one commit or two identical commits, never corruption.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context.
    portfolio_repo : PortfolioRepoResource
        Resource providing owner/name/branch/token for the portfolio repo.

    Returns
    -------
    MaterializeResult
        Metadata including commit SHA, byte sizes, and timestamps.

    Raises
    ------
    dagster.Failure
        If any file is missing locally or the GitHub API call fails.
    """

    # Collect local files written by the build asset.
    files_to_push = {
        _DAILY_PERFORMANCE_FILE: _EXPORT_DIR / _DAILY_PERFORMANCE_FILE,
    }

    missing = [name for name, path in files_to_push.items() if not path.exists()]
    if missing:
        raise Failure(
            description=(
                f"Expected local files from build asset are missing: {missing}. "
                f"Run waga_dashboard_export_build first."
            ),
        )

    auth_start = time.monotonic()
    gh = Github(portfolio_repo.token)
    try:
        repo = gh.get_repo(portfolio_repo.full_name)
    except GithubException as exc:
        raise Failure(
            description=(
                f"Failed to access portfolio repo {portfolio_repo.full_name}: "
                f"HTTP {exc.status}. Check PAT scope and repo name."
            ),
        ) from exc
    auth_ms = (time.monotonic() - auth_start) * 1000
    context.log.info("GitHub auth + repo lookup completed in %.0fms", auth_ms)

    commit_message = f"chore(dashboard): refresh data {_refresh_timestamp()}"
    commit_sha: str | None = None
    total_bytes = 0

    for rel_name, local_path in files_to_push.items():
        remote_path = f"{_REPO_DATA_DIR}/{rel_name}"
        content = local_path.read_text(encoding="utf-8")
        total_bytes += len(content)

        push_start = time.monotonic()
        try:
            # Check if file already exists; Contents API requires SHA on update.
            try:
                existing = repo.get_contents(remote_path, ref=portfolio_repo.branch)
                # get_contents can return a list if remote_path is a dir;
                # guard against that even though we expect a single file.
                if isinstance(existing, list):
                    raise Failure(
                        description=(
                            f"{remote_path} resolves to a directory in "
                            f"{portfolio_repo.full_name}, not a file."
                        ),
                    )
                result = repo.update_file(
                    path=remote_path,
                    message=commit_message,
                    content=content,
                    sha=existing.sha,
                    branch=portfolio_repo.branch,
                )
            except GithubException as exc:
                if exc.status == _HTTP_NOT_FOUND:
                    # File doesn't exist yet — create it.
                    result = repo.create_file(
                        path=remote_path,
                        message=commit_message,
                        content=content,
                        branch=portfolio_repo.branch,
                    )
                else:
                    raise
        except GithubException as exc:
            raise Failure(
                description=(
                    f"Failed to push {remote_path} to "
                    f"{portfolio_repo.full_name}: HTTP {exc.status}"
                ),
            ) from exc

        push_ms = (time.monotonic() - push_start) * 1000
        context.log.info(
            "Pushed %s in %.0fms (%d bytes)",
            remote_path,
            push_ms,
            len(content),
        )

        commit_sha = result["commit"].sha

    if commit_sha is None:
        raise Failure(description="No files were pushed; commit_sha is None.")

    return MaterializeResult(
        metadata={
            "commit_sha": commit_sha,
            "commit_url": MetadataValue.url(
                f"https://github.com/{portfolio_repo.full_name}/commit/{commit_sha}"
            ),
            "total_bytes": total_bytes,
            "file_count": len(files_to_push),
            "repo": portfolio_repo.full_name,
            "branch": portfolio_repo.branch,
        },
    )
