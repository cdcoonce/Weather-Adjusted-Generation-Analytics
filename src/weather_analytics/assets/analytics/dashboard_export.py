"""Dashboard export assets: build JSON from marts and publish to portfolio repo.

Two assets are defined here, split for clean separation of concerns:

- ``waga_dashboard_export_build`` queries Snowflake marts, lowercases column
  names (matching ``correlation.py:60-61``), projects to the UI subset, and
  writes JSON files to a local directory.
- ``waga_dashboard_export_publish`` reads the local JSON files and pushes
  them to the portfolio repo via the GitHub Git Trees API in a single commit.

Phase 1 builds a minimal tracer bullet: one JSON file with a small column
subset. Phase 2 expands the projection to the full 15-column subset and
adds the other three JSON files using the Git Trees API for a single commit.

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
    DagsterError,
    Failure,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from github import Github, InputGitTreeElement
from github.GithubException import GithubException

from weather_analytics.resources.portfolio_repo import PortfolioRepoResource
from weather_analytics.resources.snowflake import WAGASnowflakeResource


def _refresh_timestamp() -> str:
    """Return an ISO 8601 UTC timestamp suffix for commit messages."""
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MIN_ROWS = 10
_SOURCE_MART = "WAGA.MARTS.mart_asset_performance_daily"
_WEATHER_MART = "WAGA.MARTS.mart_asset_weather_performance"
_EXPORT_DIR = Path("dashboard_exports")
_SCHEMA_VERSION = "1.0"
_HTTP_NOT_FOUND = 404

_DAILY_PERFORMANCE_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "date",
    "total_net_generation_mwh",
    "daily_capacity_factor",
    "avg_availability_pct",
    "total_curtailment_mwh",
    "daily_performance_rating",
    "excellent_hours",
    "good_hours",
    "fair_hours",
    "poor_hours",
    "avg_wind_speed_mps",
    "avg_ghi",
    "avg_temperature_c",
    "data_completeness_pct",
)

_WEATHER_PERFORMANCE_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "date",
    "performance_score",
    "performance_category",
    "avg_expected_generation_mwh",
    "avg_actual_generation_mwh",
    "avg_performance_ratio_pct",
    "wind_r_squared",
    "solar_r_squared",
    "inferred_asset_type",
    "rolling_7d_avg_cf",
    "rolling_30d_avg_cf",
)

# Mart column names for the asset dimension.
# ``asset_capacity_mw`` and ``asset_size_category`` come from
# ``mart_asset_performance_daily``; ``inferred_asset_type`` comes from
# ``mart_asset_weather_performance``.  They are joined and renamed to the
# data-contract names (``capacity_mw``, ``size_category``, ``asset_type``)
# during assets_df construction below.
_DAILY_ASSET_DIM_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "asset_capacity_mw",
    "asset_size_category",
)

_DAILY_PERFORMANCE_FILE = "daily_performance.json"
_WEATHER_PERFORMANCE_FILE = "weather_performance.json"
_ASSETS_FILE = "assets.json"
_MANIFEST_FILE = "manifest.json"
_REPO_DATA_DIR = "dashboard/data"


# ===========================================================================
# Build asset: query marts, write local JSON
# ===========================================================================


@asset(
    name="waga_dashboard_export_build",
    group_name="dashboard",
    deps=["mart_asset_performance_daily", "mart_asset_weather_performance"],
)
def waga_dashboard_export_build(
    context: AssetExecutionContext,
    snowflake: WAGASnowflakeResource,
) -> MaterializeResult:
    """Query marts, project columns, write JSON files locally.

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
        Metadata including row counts, byte sizes, and output paths.

    Raises
    ------
    dagster.Failure
        If either source mart has fewer than ``_MIN_ROWS`` rows, or if
        required columns are missing.
    """
    conn = snowflake.get_connection()
    try:
        # --- Query daily performance mart ---
        query_start = time.monotonic()
        raw_daily = pl.read_database(
            query=f"SELECT * FROM {_SOURCE_MART}",
            connection=conn,
        )
        raw_daily = raw_daily.rename({col: col.lower() for col in raw_daily.columns})
        query_ms = (time.monotonic() - query_start) * 1000
        context.log.info(
            "Daily mart query completed in %.0fms (%d rows)",
            query_ms,
            raw_daily.shape[0],
        )

        if raw_daily.shape[0] < _MIN_ROWS:
            raise Failure(
                description=(
                    f"Source mart {_SOURCE_MART} has {raw_daily.shape[0]} rows, "
                    f"need at least {_MIN_ROWS}. Refusing to publish a "
                    f"broken-looking dashboard."
                ),
            )

        missing_daily = [
            c for c in _DAILY_PERFORMANCE_COLUMNS if c not in raw_daily.columns
        ]
        if missing_daily:
            raise Failure(
                description=(
                    f"Mart {_SOURCE_MART} is missing expected columns: "
                    f"{missing_daily}. Check dbt model schema."
                ),
            )

        # --- Query weather performance mart ---
        weather_start = time.monotonic()
        raw_weather = pl.read_database(
            query=f"SELECT * FROM {_WEATHER_MART}",
            connection=conn,
        )
        raw_weather = raw_weather.rename(
            {col: col.lower() for col in raw_weather.columns}
        )
        weather_ms = (time.monotonic() - weather_start) * 1000
        context.log.info(
            "Weather mart query completed in %.0fms (%d rows)",
            weather_ms,
            raw_weather.shape[0],
        )

        if raw_weather.shape[0] < _MIN_ROWS:
            raise Failure(
                description=(
                    f"Source mart {_WEATHER_MART} has {raw_weather.shape[0]} rows, "
                    f"need at least {_MIN_ROWS}. Refusing to publish a "
                    f"broken-looking dashboard."
                ),
            )

        missing_weather = [
            c for c in _WEATHER_PERFORMANCE_COLUMNS if c not in raw_weather.columns
        ]
        if missing_weather:
            raise Failure(
                description=(
                    f"Mart {_WEATHER_MART} is missing expected columns: "
                    f"{missing_weather}. Check dbt model schema."
                ),
            )

        # --- Project columns ---
        daily_df = raw_daily.select(list(_DAILY_PERFORMANCE_COLUMNS))
        weather_df = raw_weather.select(list(_WEATHER_PERFORMANCE_COLUMNS))

        # --- Build assets dimension ---
        # ``asset_capacity_mw`` and ``asset_size_category`` are in the daily
        # mart; ``inferred_asset_type`` is in the weather mart (it is a model
        # inference, not a generation concept).  Join on asset_id then rename
        # to the data-contract column names.
        daily_dim = raw_daily.select(list(_DAILY_ASSET_DIM_COLUMNS)).unique()
        weather_type = raw_weather.select(["asset_id", "inferred_asset_type"]).unique(
            subset=["asset_id"]
        )
        asset_id_suffix = pl.col("asset_id").str.split("_").list.last()
        display_name_expr = (
            pl.col("inferred_asset_type").str.to_titlecase()
            + pl.lit(" Asset ")
            + asset_id_suffix
            + pl.lit(" (")
            + pl.col("asset_capacity_mw").cast(pl.Int64).cast(pl.Utf8)
            + pl.lit(" MW)")
        )
        assets_df = (
            daily_dim.join(weather_type, on="asset_id", how="left")
            .with_columns(display_name_expr.alias("display_name"))
            .rename(
                {
                    "inferred_asset_type": "asset_type",
                    "asset_capacity_mw": "capacity_mw",
                    "asset_size_category": "size_category",
                }
            )
        )

        # --- Build manifest ---
        try:
            run_id: str = context.run.run_id
        except (AttributeError, DagsterError):
            # DagsterInvalidPropertyError (a DagsterError subclass) is raised
            # when context.run is accessed in a direct-invocation test context.
            run_id = ""
        manifest: dict[str, Any] = {
            "generated_at": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "pipeline_run_id": run_id,
            "date_range": {
                "start": str(daily_df["date"].min()),
                "end": str(daily_df["date"].max()),
            },
            "asset_count": int(assets_df.shape[0]),
            "row_counts": {
                "daily_performance": daily_df.shape[0],
                "weather_performance": weather_df.shape[0],
            },
            "schema_version": _SCHEMA_VERSION,
        }

        # --- Serialize ---
        daily_payload = json.dumps(_to_json_records(daily_df), separators=(",", ":"))
        weather_payload = json.dumps(
            _to_json_records(weather_df), separators=(",", ":")
        )
        assets_payload = json.dumps(_to_json_records(assets_df), separators=(",", ":"))
        manifest_payload = json.dumps(manifest, separators=(",", ":"))

        # --- Write files ---
        _EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        (_EXPORT_DIR / _DAILY_PERFORMANCE_FILE).write_text(
            daily_payload, encoding="utf-8"
        )
        (_EXPORT_DIR / _WEATHER_PERFORMANCE_FILE).write_text(
            weather_payload, encoding="utf-8"
        )
        (_EXPORT_DIR / _ASSETS_FILE).write_text(assets_payload, encoding="utf-8")
        (_EXPORT_DIR / _MANIFEST_FILE).write_text(manifest_payload, encoding="utf-8")

        context.log.info(
            "Wrote 4 export files to %s",
            _EXPORT_DIR,
        )

        return MaterializeResult(
            metadata={
                "daily_performance_rows": daily_df.shape[0],
                "weather_performance_rows": weather_df.shape[0],
                "asset_count": assets_df.shape[0],
                "daily_performance_bytes": len(daily_payload),
                "weather_performance_bytes": len(weather_payload),
                "assets_bytes": len(assets_payload),
                "manifest_bytes": len(manifest_payload),
                "output_dir": MetadataValue.path(str(_EXPORT_DIR)),
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
    cleaned = df.fill_nan(None)
    string_cols = [
        pl.col(c).cast(pl.Utf8) if cleaned[c].dtype.is_temporal() else pl.col(c)
        for c in cleaned.columns
    ]
    return cleaned.select(string_cols).to_dicts()


# ===========================================================================
# Publish asset: read local JSON, push to portfolio repo via Git Trees API
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

    Uses the GitHub Git Trees API via PyGithub to push all 4 files in a
    single commit. This works from Dagster Cloud serverless because it's a
    plain HTTPS request — no git clone or SSH required.

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
    files_to_push = {
        _MANIFEST_FILE: _EXPORT_DIR / _MANIFEST_FILE,
        _ASSETS_FILE: _EXPORT_DIR / _ASSETS_FILE,
        _DAILY_PERFORMANCE_FILE: _EXPORT_DIR / _DAILY_PERFORMANCE_FILE,
        _WEATHER_PERFORMANCE_FILE: _EXPORT_DIR / _WEATHER_PERFORMANCE_FILE,
    }

    missing = [n for n, p in files_to_push.items() if not p.exists()]
    if missing:
        raise Failure(
            description=f"Missing local files from build asset: {missing}",
        )

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

    try:
        ref = repo.get_git_ref(f"heads/{portfolio_repo.branch}")
        head_commit = repo.get_git_commit(ref.object.sha)

        blobs = []
        total_bytes = 0
        for rel_name, local_path in files_to_push.items():
            content = local_path.read_text(encoding="utf-8")
            total_bytes += len(content)
            blob = repo.create_git_blob(content, "utf-8")
            blobs.append(
                InputGitTreeElement(
                    path=f"{_REPO_DATA_DIR}/{rel_name}",
                    mode="100644",
                    type="blob",
                    sha=blob.sha,
                )
            )

        new_tree = repo.create_git_tree(blobs, head_commit.tree)
        try:
            run_id_pub: str = context.run.run_id
        except (AttributeError, DagsterError):
            run_id_pub = "unknown"
        date_str = datetime.now(tz=UTC).strftime("%Y-%m-%d")
        commit_message = (
            f"chore(dashboard): refresh data {date_str} [pipeline run {run_id_pub}]"
        )
        new_commit = repo.create_git_commit(
            message=commit_message,
            tree=new_tree,
            parents=[head_commit],
        )
        ref.edit(sha=new_commit.sha)
        commit_sha = new_commit.sha

    except GithubException as exc:
        raise Failure(
            description=(
                f"Failed to push dashboard data to "
                f"{portfolio_repo.full_name}: HTTP {exc.status}"
            ),
        ) from exc

    context.log.info("Pushed 4 files in single commit %s", commit_sha)

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
