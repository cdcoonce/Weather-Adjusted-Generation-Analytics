"""Dagster jobs for renewable energy pipeline."""

from dagster import AssetSelection, define_asset_job

# Daily ingestion job - now uses combined ingestion to avoid concurrency issues
daily_ingestion_job = define_asset_job(
    name="daily_ingestion_job",
    description="Daily ingestion of weather and generation data (combined to avoid DuckDB concurrency)",
    selection=AssetSelection.assets("combined_ingestion"),
)

# dbt transformation job
daily_dbt_job = define_asset_job(
    name="daily_dbt_job",
    description="Daily dbt transformations",
    selection=AssetSelection.all(),  # Will include dbt assets when configured
)

# Correlation analysis job
correlation_job = define_asset_job(
    name="correlation_job",
    description="Weather-generation correlation analysis",
    selection=AssetSelection.groups("analytics"),
)

__all__ = [
    "daily_ingestion_job",
    "daily_dbt_job",
    "correlation_job",
]
