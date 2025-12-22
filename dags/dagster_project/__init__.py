"""Dagster project for renewable energy analytics pipeline."""

from dagster import Definitions, load_assets_from_modules

from . import assets
from .jobs import (
    correlation_job,
    daily_dbt_job,
    daily_ingestion_job,
)
from .resources import dlt_resource, duckdb_resource
from .schedules import (
    daily_ingestion_schedule,
    weekly_performance_schedule,
)
from .sensors import new_data_file_sensor

# Load all assets
all_assets = load_assets_from_modules([assets])

# Define repository
defs = Definitions(
    assets=all_assets,
    jobs=[
        daily_ingestion_job,
        daily_dbt_job,
        correlation_job,
    ],
    schedules=[
        daily_ingestion_schedule,
        weekly_performance_schedule,
    ],
    sensors=[
        new_data_file_sensor,
    ],
    resources={
        "duckdb": duckdb_resource,
        "dlt": dlt_resource,
    },
)
