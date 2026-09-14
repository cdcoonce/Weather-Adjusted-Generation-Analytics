"""Dagster Definitions for the Weather Analytics pipeline.

This is the entry point Dagster loads: the ``weather_analytics.definitions``
gRPC code-location module served by ``dagster api grpc`` in the Docker image
running on the Dagster OSS webserver/daemon (home server), and also
``dagster dev`` for the local UI. All assets, resources, jobs, and
schedules are registered here. The macOS launchd CLI runs
(``scripts/run_scheduled.py``) remain a manual fallback — see
``docs/local-scheduling.md``.
"""

from __future__ import annotations

from dagster import Definitions, EnvVar
from dagster_dbt import DbtCliResource

from weather_analytics.assets.analytics import (
    waga_correlation_analysis,
    waga_dashboard_export_build,
    waga_dashboard_publish,
)
from weather_analytics.assets.dbt_assets import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    waga_dbt_assets,
)
from weather_analytics.assets.ingestion import (
    waga_generation_ingestion,
    waga_weather_ingestion,
)
from weather_analytics.checks import (
    waga_generation_freshness_check,
    waga_generation_value_range_check,
    waga_mart_correlation_row_count_check,
    waga_mart_performance_row_count_check,
    waga_raw_generation_row_count_check,
    waga_raw_weather_row_count_check,
    waga_weather_freshness_check,
    waga_weather_value_range_check,
)
from weather_analytics.resources.dlt_resource import DltIngestionResource
from weather_analytics.resources.snowflake import WAGASnowflakeResource
from weather_analytics.schedules import (
    waga_daily_job,
    waga_daily_job_schedule,
    waga_weekly_job,
    waga_weekly_job_schedule,
)

defs = Definitions(
    assets=[
        asset
        for asset in [
            waga_weather_ingestion,
            waga_generation_ingestion,
            waga_dbt_assets,
            waga_correlation_analysis,
            waga_dashboard_export_build,
            waga_dashboard_publish,
        ]
        if asset is not None
    ],
    asset_checks=[
        waga_weather_freshness_check,
        waga_generation_freshness_check,
        waga_raw_weather_row_count_check,
        waga_raw_generation_row_count_check,
        waga_mart_performance_row_count_check,
        waga_mart_correlation_row_count_check,
        waga_weather_value_range_check,
        waga_generation_value_range_check,
    ],
    jobs=[waga_daily_job, waga_weekly_job],
    schedules=[
        waga_daily_job_schedule,
        waga_weekly_job_schedule,
    ],
    resources={
        "snowflake": WAGASnowflakeResource(
            account=EnvVar("WAGA_SNOWFLAKE_ACCOUNT"),
            user=EnvVar("WAGA_SNOWFLAKE_USER"),
            private_key_base64=EnvVar("WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64"),
            warehouse=EnvVar("WAGA_SNOWFLAKE_WAREHOUSE"),
            database=EnvVar("WAGA_SNOWFLAKE_DATABASE"),
            role=EnvVar("WAGA_SNOWFLAKE_ROLE"),
        ),
        "dlt_ingestion": DltIngestionResource(
            pipeline_name=EnvVar("WAGA_DLT_PIPELINE_NAME"),
            dataset_name=EnvVar("WAGA_DLT_DATASET_NAME"),
            snowflake_account=EnvVar("WAGA_SNOWFLAKE_ACCOUNT"),
            snowflake_user=EnvVar("WAGA_SNOWFLAKE_USER"),
            snowflake_private_key_base64=EnvVar("WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64"),
            snowflake_warehouse=EnvVar("WAGA_SNOWFLAKE_WAREHOUSE"),
            snowflake_database=EnvVar("WAGA_SNOWFLAKE_DATABASE"),
            snowflake_role=EnvVar("WAGA_SNOWFLAKE_ROLE"),
        ),
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
        ),
    },
)
