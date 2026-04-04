"""Dagster asset checks for WAGA data quality.

Three categories of checks:
- **Freshness**: ensures data is no older than a configurable threshold.
- **Row count**: verifies minimum expected rows per table.
- **Value range**: validates physical plausibility of key columns.
"""

from datetime import UTC, datetime, timedelta

from dagster import AssetCheckResult, AssetKey, asset_check

from weather_analytics.resources.snowflake import WAGASnowflakeResource

# ---------------------------------------------------------------------------
# Configurable thresholds
# ---------------------------------------------------------------------------
FRESHNESS_THRESHOLD_HOURS: int = 48
MIN_ROW_COUNT: int = 100

# Value range limits (inclusive)
WIND_SPEED_MIN: float = 0.0
WIND_SPEED_MAX: float = 50.0
TEMPERATURE_C_MIN: float = -50.0
TEMPERATURE_C_MAX: float = 60.0
RELATIVE_HUMIDITY_MIN: float = 0.0
RELATIVE_HUMIDITY_MAX: float = 100.0
GENERATION_MWH_MIN: float = 0.0
CAPACITY_FACTOR_MIN: float = 0.0
CAPACITY_FACTOR_MAX: float = 1.0


# ===================================================================
# Freshness checks
# ===================================================================


@asset_check(asset=AssetKey(["waga_dbt_assets"]), name="waga_weather_freshness_check")
def waga_weather_freshness_check(
    snowflake: WAGASnowflakeResource,
) -> AssetCheckResult:
    """Check that stg_weather data is no older than the freshness threshold."""
    conn = snowflake.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM WAGA.STAGING.stg_weather")
        row = cursor.fetchone()
        max_ts: datetime | None = row[0] if row else None

        if max_ts is None:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": "No rows in stg_weather"},
            )

        if max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=UTC)

        cutoff = datetime.now(tz=UTC) - timedelta(hours=FRESHNESS_THRESHOLD_HOURS)
        passed = max_ts >= cutoff
        return AssetCheckResult(
            passed=passed,
            metadata={
                "max_timestamp": str(max_ts),
                "threshold_hours": FRESHNESS_THRESHOLD_HOURS,
            },
        )
    finally:
        conn.close()


@asset_check(
    asset=AssetKey(["waga_dbt_assets"]), name="waga_generation_freshness_check"
)
def waga_generation_freshness_check(
    snowflake: WAGASnowflakeResource,
) -> AssetCheckResult:
    """Check that stg_generation data is no older than the freshness threshold."""
    conn = snowflake.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM WAGA.STAGING.stg_generation")
        row = cursor.fetchone()
        max_ts: datetime | None = row[0] if row else None

        if max_ts is None:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": "No rows in stg_generation"},
            )

        if max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=UTC)

        cutoff = datetime.now(tz=UTC) - timedelta(hours=FRESHNESS_THRESHOLD_HOURS)
        passed = max_ts >= cutoff
        return AssetCheckResult(
            passed=passed,
            metadata={
                "max_timestamp": str(max_ts),
                "threshold_hours": FRESHNESS_THRESHOLD_HOURS,
            },
        )
    finally:
        conn.close()


# ===================================================================
# Row count checks
# ===================================================================


@asset_check(
    asset=AssetKey(["waga_weather_ingestion"]),
    name="waga_raw_weather_row_count_check",
)
def waga_raw_weather_row_count_check(
    snowflake: WAGASnowflakeResource,
) -> AssetCheckResult:
    """Verify raw weather table has at least MIN_ROW_COUNT rows."""
    return _row_count_check(snowflake, "WAGA.RAW.raw_weather", MIN_ROW_COUNT)


@asset_check(
    asset=AssetKey(["waga_generation_ingestion"]),
    name="waga_raw_generation_row_count_check",
)
def waga_raw_generation_row_count_check(
    snowflake: WAGASnowflakeResource,
) -> AssetCheckResult:
    """Verify raw generation table has at least MIN_ROW_COUNT rows."""
    return _row_count_check(snowflake, "WAGA.RAW.raw_generation", MIN_ROW_COUNT)


@asset_check(
    asset=AssetKey(["waga_dbt_assets"]),
    name="waga_mart_performance_row_count_check",
)
def waga_mart_performance_row_count_check(
    snowflake: WAGASnowflakeResource,
) -> AssetCheckResult:
    """Verify mart_asset_performance_daily has at least MIN_ROW_COUNT rows."""
    return _row_count_check(
        snowflake,
        "WAGA.MARTS.mart_asset_performance_daily",
        MIN_ROW_COUNT,
    )


@asset_check(
    asset=AssetKey(["waga_correlation_analysis"]),
    name="waga_mart_correlation_row_count_check",
)
def waga_mart_correlation_row_count_check(
    snowflake: WAGASnowflakeResource,
) -> AssetCheckResult:
    """Verify correlation analysis output has at least MIN_ROW_COUNT rows."""
    return _row_count_check(
        snowflake,
        "WAGA.ANALYTICS.correlation_analysis",
        MIN_ROW_COUNT,
    )


def _row_count_check(
    snowflake: WAGASnowflakeResource,
    table_fqn: str,
    minimum: int,
) -> AssetCheckResult:
    """Shared helper that counts rows and compares to a minimum.

    Parameters
    ----------
    snowflake : WAGASnowflakeResource
        Snowflake resource providing ``get_connection()``.
    table_fqn : str
        Fully qualified table name (``DB.SCHEMA.TABLE``).
    minimum : int
        Minimum acceptable row count.

    Returns
    -------
    AssetCheckResult
        Passed if row count >= minimum.
    """
    conn = snowflake.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_fqn}")
        row_count: int = cursor.fetchone()[0]
        return AssetCheckResult(
            passed=row_count >= minimum,
            metadata={"row_count": row_count, "minimum": minimum},
        )
    finally:
        conn.close()


# ===================================================================
# Value range checks
# ===================================================================


@asset_check(
    asset=AssetKey(["waga_dbt_assets"]),
    name="waga_weather_value_range_check",
)
def waga_weather_value_range_check(
    snowflake: WAGASnowflakeResource,
) -> AssetCheckResult:
    """Validate physical plausibility of weather columns.

    Checks wind_speed, temperature_c, and relative_humidity against
    physically reasonable bounds.
    """
    conn = snowflake.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT COUNT(*) AS violations
            FROM WAGA.STAGING.stg_weather
            WHERE wind_speed < {WIND_SPEED_MIN}
               OR wind_speed > {WIND_SPEED_MAX}
               OR temperature_c < {TEMPERATURE_C_MIN}
               OR temperature_c > {TEMPERATURE_C_MAX}
               OR relative_humidity < {RELATIVE_HUMIDITY_MIN}
               OR relative_humidity > {RELATIVE_HUMIDITY_MAX}
            """
        )
        violations: int = cursor.fetchone()[0]
        return AssetCheckResult(
            passed=violations == 0,
            metadata={"out_of_range_rows": violations},
        )
    finally:
        conn.close()


@asset_check(
    asset=AssetKey(["waga_dbt_assets"]),
    name="waga_generation_value_range_check",
)
def waga_generation_value_range_check(
    snowflake: WAGASnowflakeResource,
) -> AssetCheckResult:
    """Validate physical plausibility of generation columns.

    Checks generation_mwh >= 0 and capacity_factor in [0, 1].
    """
    conn = snowflake.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT COUNT(*) AS violations
            FROM WAGA.STAGING.stg_generation
            WHERE generation_mwh < {GENERATION_MWH_MIN}
               OR capacity_factor < {CAPACITY_FACTOR_MIN}
               OR capacity_factor > {CAPACITY_FACTOR_MAX}
            """
        )
        violations: int = cursor.fetchone()[0]
        return AssetCheckResult(
            passed=violations == 0,
            metadata={"out_of_range_rows": violations},
        )
    finally:
        conn.close()
