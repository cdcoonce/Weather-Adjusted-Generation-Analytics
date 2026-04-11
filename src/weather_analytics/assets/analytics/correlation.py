"""Correlation analytics asset for weather-generation analysis.

Reads from the dbt mart, computes correlation features using the
LazyFrame-based polars utilities, and writes results to the
WAGA.ANALYTICS schema in Snowflake.
"""

import polars as pl
from dagster import AssetExecutionContext, Failure, MaterializeResult, asset

from weather_analytics.lib.polars_utils import (
    add_lag_features,
    add_rolling_stats,
    calculate_correlation,
)
from weather_analytics.resources.snowflake import WAGASnowflakeResource

_MIN_ROWS = 10
_SOURCE_TABLE = "WAGA.MARTS.mart_asset_performance_daily"
_TARGET_SCHEMA = "ANALYTICS"
_TARGET_TABLE = "WAGA.ANALYTICS.correlation_results"


@asset(
    name="waga_correlation_analysis",
    group_name="waga_analytics",
    deps=["mart_asset_performance_daily"],
)
def waga_correlation_analysis(
    context: AssetExecutionContext,
    snowflake: WAGASnowflakeResource,
) -> MaterializeResult:
    """Compute weather-generation correlations and write to ANALYTICS schema.

    Parameters
    ----------
    context
        Dagster execution context for logging.
    snowflake
        WAGA Snowflake resource providing authenticated connections.

    Returns
    -------
    MaterializeResult
        Metadata including row count and mean correlation.

    Raises
    ------
    dagster.Failure
        If the source mart contains fewer than ``_MIN_ROWS`` rows.
    """
    conn = snowflake.get_connection()

    try:
        raw_df = pl.read_database(
            query=f"SELECT * FROM {_SOURCE_TABLE}",
            connection=conn,
        )
        # Snowflake returns UPPERCASE column names; normalize to lowercase
        # so Polars references match the dbt model definitions.
        raw_df = raw_df.rename({col: col.lower() for col in raw_df.columns})

        if raw_df.shape[0] < _MIN_ROWS:
            raise Failure(
                description=(
                    f"Source mart has {raw_df.shape[0]} rows, "
                    f"need at least {_MIN_ROWS}."
                ),
            )

        context.log.info("Read %d rows from %s", raw_df.shape[0], _SOURCE_TABLE)

        lf = raw_df.lazy()

        # Static correlation per asset
        corr_lf = calculate_correlation(
            lf,
            col1="total_net_generation_mwh",
            col2="avg_temperature_c",
            partition_by="asset_id",
        )

        # Enrich with lag features and rolling stats
        enriched_lf = add_lag_features(
            lf,
            column="total_net_generation_mwh",
            lags=[1, 3, 7],
            partition_by="asset_id",
        )
        enriched_lf = add_rolling_stats(
            enriched_lf,
            column="total_net_generation_mwh",
            window_sizes=[7, 30],
            stats=["mean", "std"],
            partition_by="asset_id",
        )

        corr_df = corr_lf.collect()
        enriched_df = enriched_lf.collect()

        context.log.info(
            "Correlation results: %d rows; enriched: %d rows",
            corr_df.shape[0],
            enriched_df.shape[0],
        )

        # Write results to Snowflake ANALYTICS schema
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_TARGET_SCHEMA}")
        _write_polars_to_snowflake(corr_df, _TARGET_TABLE, cursor)

        corr_col = "corr_total_net_generation_mwh_avg_temperature_c"
        mean_corr = corr_df[corr_col].mean() if corr_col in corr_df.columns else None

        return MaterializeResult(
            metadata={
                "row_count": corr_df.shape[0],
                "enriched_row_count": enriched_df.shape[0],
                "mean_correlation": float(mean_corr) if mean_corr is not None else None,
            },
        )
    finally:
        conn.close()


_POLARS_TO_SNOWFLAKE_TYPES: dict[type, str] = {
    pl.Float64: "FLOAT",
    pl.Float32: "FLOAT",
    pl.Int64: "NUMBER",
    pl.Int32: "NUMBER",
    pl.Int16: "NUMBER",
    pl.Int8: "NUMBER",
    pl.UInt64: "NUMBER",
    pl.UInt32: "NUMBER",
    pl.Boolean: "BOOLEAN",
    pl.Date: "DATE",
    pl.Datetime: "TIMESTAMP_NTZ",
    pl.Utf8: "VARCHAR",
    pl.String: "VARCHAR",
}


def _sf_type(dtype: pl.DataType) -> str:
    """Map a Polars dtype to a Snowflake column type."""
    return _POLARS_TO_SNOWFLAKE_TYPES.get(type(dtype), "VARCHAR")


def _write_polars_to_snowflake(
    df: pl.DataFrame,
    table: str,
    cursor: object,
) -> None:
    """Write a Polars DataFrame to a Snowflake table via INSERT.

    Uses CREATE OR REPLACE TABLE inside an explicit transaction so
    an interrupted write does not leave a half-populated table.

    Parameters
    ----------
    df
        DataFrame to write.
    table
        Fully qualified Snowflake table name.
    cursor
        Active Snowflake cursor.
    """
    if df.is_empty():
        return

    # Replace NaN with None (SQL NULL) — Snowflake rejects NaN literals.
    df = df.fill_nan(None)

    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    create_cols = ", ".join(f"{col} {_sf_type(df[col].dtype)}" for col in df.columns)

    cursor.execute("BEGIN")  # type: ignore[union-attr]
    try:
        cursor.execute(  # type: ignore[union-attr]
            f"CREATE OR REPLACE TABLE {table} ({create_cols})"
        )
        insert_sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        rows = df.rows()
        cursor.executemany(insert_sql, rows)  # type: ignore[union-attr]
        cursor.execute("COMMIT")  # type: ignore[union-attr]
    except Exception:
        cursor.execute("ROLLBACK")  # type: ignore[union-attr]
        raise
