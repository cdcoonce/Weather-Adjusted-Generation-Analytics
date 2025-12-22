"""Dagster assets for renewable energy data pipeline."""

from dagster import AssetExecutionContext, asset

from weather_adjusted_generation_analytics.config import config
from weather_adjusted_generation_analytics.loaders import (
    run_generation_ingestion,
    run_weather_ingestion,
)
from weather_adjusted_generation_analytics.utils import get_logger

logger = get_logger(__name__)


@asset(
    name="combined_ingestion",
    group_name="ingestion",
    description="Combined weather and generation data ingestion to DuckDB",
)
def combined_ingestion_asset(context: AssetExecutionContext) -> None:
    """
    Ingest both weather and generation data using dlt.

    Loads both datasets sequentially to avoid DuckDB concurrency issues.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context

    Returns
    -------
    None

    """
    context.log.info("Starting combined data ingestion")

    try:
        # Run weather ingestion first
        context.log.info("Running weather data ingestion")
        run_weather_ingestion()
        context.log.info("Weather data ingestion completed")

        # Run generation ingestion second
        context.log.info("Running generation data ingestion")
        run_generation_ingestion()
        context.log.info("Generation data ingestion completed")

        context.log.info("Combined data ingestion completed successfully")

    except Exception as e:
        context.log.error(f"Combined data ingestion failed: {e}")
        raise


# Keep individual assets for flexibility but mark as deprecated
@asset(
    name="weather_data",
    group_name="ingestion",
    description="Weather data ingested from Parquet files to DuckDB (use combined_ingestion instead)",
    deps=[],  # No dependencies since it runs independently
)
def weather_asset(context: AssetExecutionContext) -> None:
    """
    Ingest weather data using dlt.

    DEPRECATED: Use combined_ingestion_asset instead to avoid concurrency issues.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context

    Returns
    -------
    None

    """
    context.log.warning("weather_asset is deprecated. Use combined_ingestion_asset instead.")

    try:
        run_weather_ingestion()
        context.log.info("Weather data ingestion completed successfully")
    except Exception as e:
        context.log.error(f"Weather data ingestion failed: {e}")
        raise


@asset(
    name="generation_data",
    group_name="ingestion",
    description="Generation data ingested from Parquet files to DuckDB (use combined_ingestion instead)",
    deps=[],  # No dependencies since it runs independently
)
def generation_asset(context: AssetExecutionContext) -> None:
    """
    Ingest generation data using dlt.

    DEPRECATED: Use combined_ingestion_asset instead to avoid concurrency issues.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context

    Returns
    -------
    None

    """
    context.log.warning("generation_asset is deprecated. Use combined_ingestion_asset instead.")

    try:
        run_generation_ingestion()
        context.log.info("Generation data ingestion completed successfully")
    except Exception as e:
        context.log.error(f"Generation data ingestion failed: {e}")
        raise


@asset(
    name="weather_generation_correlation",
    group_name="analytics",
    description="Weather and generation correlation analysis using Polars",
    deps=["combined_ingestion"],  # Updated dependency
)
def correlation_asset(context: AssetExecutionContext) -> dict:
    """
    Calculate correlations between weather and generation.

    Uses Polars to compute correlation matrices and regression
    parameters for weather-generation relationships.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context

    Returns
    -------
    dict
        Correlation statistics and metrics

    """
    import duckdb
    import polars as pl

    context.log.info("Starting correlation analysis")

    try:
        # Connect to DuckDB
        conn = duckdb.connect(str(config.duckdb_path))

        # Load data using Polars
        query = f"""
        SELECT
            g.asset_id,
            g.timestamp,
            g.net_generation_mwh,
            g.asset_capacity_mw,
            w.wind_speed_mps,
            w.ghi
        FROM {config.dlt_schema}.generation g
        INNER JOIN {config.dlt_schema}.weather w
            ON g.asset_id = w.asset_id
            AND g.timestamp = w.timestamp
        """

        df = pl.from_arrow(conn.execute(query).fetch_arrow_table())
        conn.close()

        context.log.info(f"Loaded {len(df)} records for correlation analysis")

        # Calculate correlations by asset
        correlations = df.group_by("asset_id").agg([
            pl.corr("wind_speed_mps", "net_generation_mwh").alias("wind_corr"),
            pl.corr("ghi", "net_generation_mwh").alias("solar_corr"),
            pl.len().alias("observation_count"),
        ])

        context.log.info("Correlation results:")
        context.log.info(str(correlations))

        # Convert to dict for return
        result = correlations.to_dicts()

        context.log.info("Correlation analysis completed successfully")

        return {"correlations": result, "total_records": len(df)}

    except Exception as e:
        context.log.error(f"Correlation analysis failed: {e}")
        raise


__all__ = [
    "combined_ingestion_asset",
    "weather_asset",
    "generation_asset",
    "correlation_asset",
]
