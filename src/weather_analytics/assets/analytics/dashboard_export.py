"""Dashboard export asset: build JSON exports from marts.

``waga_dashboard_export_build`` queries Snowflake marts, lowercases column
names (matching ``correlation.py:60-61``), projects to the UI subset, and
writes JSON files to a local directory for the static cockpit dashboard to
render.
"""

import json
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
    asset,
)
from snowflake.connector import SnowflakeConnection

from weather_analytics.resources.snowflake import WAGASnowflakeResource

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MIN_ROWS = 10
_SOURCE_MART = "WAGA.MARTS.mart_asset_performance_daily"
_WEATHER_MART = "WAGA.MARTS.mart_asset_weather_performance"
_DIM_MART = "WAGA.MARTS.dim_asset"
_EXPORT_DIR = Path("dashboard_exports")
_SCHEMA_VERSION = "2.0"

_DAILY_PERFORMANCE_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "date",
    "asset_type",
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
    # Technology-specific (null where not applicable).
    "avg_soc_pct",
    "total_charge_mwh",
    "total_discharge_mwh",
    "total_fuel_mmbtu",
    "avg_heat_rate_btu_kwh",
    "total_co2_tonnes",
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

# Columns projected from the ``dim_asset`` mart to build assets.json. Provides
# real site names, coordinates, and region; renamed to the data-contract names
# (``name``, ``capacity_mw``, ``size_category``) during assets_df construction.
_DIM_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "asset_name",
    "asset_type",
    "asset_capacity_mw",
    "asset_size_category",
    "latitude",
    "longitude",
    "region",
)

_DAILY_PERFORMANCE_FILE = "daily_performance.json"
_WEATHER_PERFORMANCE_FILE = "weather_performance.json"
_ASSETS_FILE = "assets.json"
_MANIFEST_FILE = "manifest.json"


def _query_and_validate(
    conn: SnowflakeConnection,
    mart: str,
    required_columns: tuple[str, ...],
    *,
    min_rows: int | None,
    context: AssetExecutionContext,
) -> pl.DataFrame:
    """Query a mart, lowercase columns, and validate row count + schema.

    Parameters
    ----------
    conn : SnowflakeConnection
        Open Snowflake connection.
    mart : str
        Fully-qualified mart/table name.
    required_columns : tuple[str, ...]
        Columns that must be present (post-lowercasing).
    min_rows : int | None
        Minimum acceptable row count, or ``None`` to skip the check.
    context : AssetExecutionContext
        For logging.

    Returns
    -------
    pl.DataFrame
        The queried frame with lowercased column names.

    Raises
    ------
    dagster.Failure
        If the row count is below ``min_rows`` or required columns are missing.
    """
    df = pl.read_database(query=f"SELECT * FROM {mart}", connection=conn)
    df = df.rename({col: col.lower() for col in df.columns})
    context.log.info("%s query completed (%d rows)", mart, df.shape[0])

    if min_rows is not None and df.shape[0] < min_rows:
        raise Failure(
            description=(
                f"Source mart {mart} has {df.shape[0]} rows, need at least "
                f"{min_rows}. Refusing to publish a broken-looking dashboard."
            ),
        )
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise Failure(
            description=(
                f"Mart {mart} is missing expected columns: {missing}. "
                f"Check dbt model schema."
            ),
        )
    return df


# ===========================================================================
# Build asset: query marts, write local JSON
# ===========================================================================


@asset(
    name="waga_dashboard_export_build",
    group_name="dashboard",
    deps=[
        "mart_asset_performance_daily",
        "mart_asset_weather_performance",
        "dim_asset",
    ],
)
def waga_dashboard_export_build(
    context: AssetExecutionContext,
    snowflake: WAGASnowflakeResource,
) -> MaterializeResult:
    """Query marts, project columns, write JSON files locally.

    The static cockpit dashboard (``weather_analytics.cockpit``) reads these
    local files to render and deploy the dashboard.

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
        # --- Query + validate the marts ---
        raw_daily = _query_and_validate(
            conn, _SOURCE_MART, _DAILY_PERFORMANCE_COLUMNS,
            min_rows=_MIN_ROWS, context=context,
        )
        raw_weather = _query_and_validate(
            conn, _WEATHER_MART, _WEATHER_PERFORMANCE_COLUMNS,
            min_rows=_MIN_ROWS, context=context,
        )
        raw_dim = _query_and_validate(
            conn, _DIM_MART, _DIM_COLUMNS, min_rows=None, context=context,
        )

        # --- Project columns ---
        daily_df = raw_daily.select(list(_DAILY_PERFORMANCE_COLUMNS))
        weather_df = raw_weather.select(list(_WEATHER_PERFORMANCE_COLUMNS))

        # --- Build assets dimension ---
        # Real site name, capacity, size, coordinates, and region come from the
        # ``dim_asset`` mart (seeded from the fleet registry). Compute a display
        # name and rename to the data-contract names.
        dim = raw_dim.select(list(_DIM_COLUMNS)).unique(subset=["asset_id"])
        display_name_expr = (
            pl.col("asset_name")
            + pl.lit(" (")
            + pl.col("asset_capacity_mw").cast(pl.Int64).cast(pl.Utf8)
            + pl.lit(" MW ")
            + pl.col("asset_type")
            + pl.lit(")")
        )
        assets_df = dim.with_columns(
            display_name_expr.alias("display_name")
        ).rename(
            {
                "asset_name": "name",
                "asset_capacity_mw": "capacity_mw",
                "asset_size_category": "size_category",
            }
        )
        asset_type_counts = {
            str(row["asset_type"]): int(row["count"])
            for row in assets_df.group_by("asset_type").len(name="count").iter_rows(
                named=True
            )
        }

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
            "asset_type_counts": asset_type_counts,
            "weather_source": "snowflake",
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
