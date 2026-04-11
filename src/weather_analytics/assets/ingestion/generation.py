"""Generation ingestion asset — generates mock data into Snowflake RAW."""

import time
from collections.abc import Iterator

import dlt
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Failure,
    MaterializeResult,
    asset,
)

from weather_analytics.mock_data.generate_generation import (
    ASSET_CONFIGS,
    generate_generation_data,
)
from weather_analytics.resources.dlt_resource import DltIngestionResource

GENERATION_PARTITIONS = DailyPartitionsDefinition(start_date="2023-01-01")


@dlt.resource(
    name="generation",
    write_disposition="merge",
    primary_key=["asset_id", "timestamp"],
)
def _generation_dlt_resource(
    records: list[dict[str, object]],
) -> Iterator[dict[str, object]]:
    """dlt resource that yields generation records for Snowflake merge.

    Parameters
    ----------
    records : list[dict[str, object]]
        Pre-generated generation records.

    Yields
    ------
    dict[str, object]
        Individual generation records.
    """
    yield from records


@asset(
    name="waga_generation_ingestion",
    group_name="waga_ingestion",
    partitions_def=GENERATION_PARTITIONS,
    op_tags={"dagster/concurrency_key": "waga_ingestion"},
)
def waga_generation_ingestion(
    context: AssetExecutionContext,
    dlt_ingestion: DltIngestionResource,
) -> MaterializeResult:
    """Generate mock generation data for a single day and load into Snowflake RAW.

    Reads ``context.partition_key`` to determine the target date, generates
    hourly records for all configured assets, and merges into Snowflake via
    dlt on composite key ``(asset_id, timestamp)``.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context (provides partition key).
    dlt_ingestion : DltIngestionResource
        Configured dlt resource providing Snowflake pipeline.

    Returns
    -------
    MaterializeResult
        Dagster result with load metadata.

    Raises
    ------
    Failure
        If the generator produces zero rows.
    """
    partition_key = context.partition_key
    start = f"{partition_key}T00:00:00"
    end = f"{partition_key}T23:00:00"

    df = generate_generation_data(
        start_date=start,
        end_date=end,
        random_seed=int(time.time()),
    )
    row_count = len(df)
    asset_count = len(ASSET_CONFIGS)

    if row_count == 0:
        raise Failure(
            description=f"Generator returned 0 rows for partition {partition_key}",
        )

    context.log.info(
        "Generated %d rows for %d assets on partition %s",
        row_count,
        asset_count,
        partition_key,
    )

    records = df.to_dicts()
    pipeline = dlt_ingestion.create_pipeline()
    generation_data = _generation_dlt_resource(records=records)
    load_info = pipeline.run(generation_data)

    # Extract metadata for Dagster UI
    load_id = load_info.loads_ids[0] if load_info.loads_ids else "no_load_id"
    has_failed = load_info.has_failed_jobs

    # Count loaded rows and schema changes
    rows_loaded = (
        load_info.metrics.get("rows_loaded", 0) if hasattr(load_info, "metrics") else 0
    )
    if rows_loaded == 0:
        for package in load_info.load_packages:
            for job in package.jobs.get("completed_jobs", []):
                rows_loaded += getattr(job, "rows_count", 0)
    schema_changes: list[str] = []
    for package in load_info.load_packages:
        schema_update = package.schema_update
        if schema_update:
            schema_changes.extend(list(schema_update.keys()))

    if has_failed:
        context.log.error("Generation ingestion had failed jobs")
        for package in load_info.load_packages:
            failed_jobs = package.jobs.get("failed_jobs", [])
            for job in failed_jobs:
                context.log.error(
                    "Failed: %s — %s",
                    job.job_file_path,
                    job.failed_message,
                )
    else:
        context.log.info("Generation ingestion completed: load_id=%s", load_id)

    return MaterializeResult(
        metadata={
            "load_id": load_id,
            "partition_key": partition_key,
            "rows_generated": row_count,
            "rows_loaded": rows_loaded,
            "has_failed_jobs": has_failed,
            "schema_changes": schema_changes,
        },
    )
