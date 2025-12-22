"""Phase 5 end-to-end smoke test.

This test validates a minimal "raw -> dbt mart" workflow against a temp DuckDB.
We seed the raw ingestion tables directly (rather than running dlt) to avoid
creating dlt pipeline state in the repository.

"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime

import duckdb
import polars as pl
import pytest

from tests.fixtures.dbt_cli import DbtInvocation, run_dbt, write_temp_profiles_yml
from tests.fixtures.duckdb_fixtures import load_weather_and_generation


def _weather_df_for_dbt() -> pl.DataFrame:
    rows = [
        {
            "timestamp": datetime(2023, 1, 1, 0, 0, 0),
            "asset_id": "asset_001",
            "wind_speed_mps": 5.0,
            "wind_direction_deg": 180.0,
            "ghi": 200.0,
            "temperature_c": 15.0,
            "pressure_hpa": 1013.25,
            "relative_humidity": 50.0,
            "_dlt_load_id": 1,
        },
        {
            "timestamp": datetime(2023, 1, 1, 1, 0, 0),
            "asset_id": "asset_001",
            "wind_speed_mps": 6.0,
            "wind_direction_deg": 180.0,
            "ghi": 250.0,
            "temperature_c": 16.0,
            "pressure_hpa": 1013.00,
            "relative_humidity": 48.0,
            "_dlt_load_id": 1,
        },
    ]
    return pl.DataFrame(rows).with_columns(
        [
            pl.col("timestamp").cast(pl.Datetime),
            pl.col("asset_id").cast(pl.Utf8),
        ]
    )


def _generation_df_for_dbt() -> pl.DataFrame:
    rows = [
        {
            "timestamp": datetime(2023, 1, 1, 0, 0, 0),
            "asset_id": "asset_001",
            "gross_generation_mwh": 51.0,
            "net_generation_mwh": 50.0,
            "curtailment_mwh": 0.0,
            "availability_pct": 99.0,
            "asset_capacity_mw": 100.0,
        },
        {
            "timestamp": datetime(2023, 1, 1, 1, 0, 0),
            "asset_id": "asset_001",
            "gross_generation_mwh": 52.0,
            "net_generation_mwh": 51.0,
            "curtailment_mwh": 0.0,
            "availability_pct": 99.0,
            "asset_capacity_mw": 100.0,
        },
    ]
    return pl.DataFrame(rows).with_columns(
        [
            pl.col("timestamp").cast(pl.Datetime),
            pl.col("asset_id").cast(pl.Utf8),
        ]
    )


@pytest.mark.integration
@pytest.mark.dbt
@pytest.mark.duckdb
@pytest.mark.io
def test_e2e_dbt_build_mart_and_query_rowcount(tmp_path: Path) -> None:
    project_dir = Path(__file__).resolve().parents[2] / "dbt" / "renewable_dbt"

    if not (project_dir / "dbt_packages" / "dbt_utils").exists():
        pytest.skip("dbt packages missing; run `uv run dbt deps` in dbt/renewable_dbt")

    duckdb_path = tmp_path / "warehouse.duckdb"
    profiles_dir = tmp_path / "dbt_profiles"
    target_path = tmp_path / "dbt_target"

    write_temp_profiles_yml(profiles_dir=profiles_dir, duckdb_path=duckdb_path)

    con = duckdb.connect(str(duckdb_path))
    try:
        load_weather_and_generation(
            conn=con,
            weather_df=_weather_df_for_dbt(),
            generation_df=_generation_df_for_dbt(),
        )
    finally:
        con.close()

    invocation = DbtInvocation(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target_path=target_path,
        target="dev",
    )

    # Build one mart to prove the core pipeline works.
    run_dbt(
        "build",
        "--select",
        "+marts.mart_asset_performance_daily",
        invocation=invocation,
    )

    con = duckdb.connect(str(duckdb_path))
    try:
        schema = con.execute(
            "SELECT table_schema "
            "FROM information_schema.tables "
            "WHERE table_name = 'mart_asset_performance_daily' "
            "LIMIT 1"
        ).fetchone()[0]

        count = con.execute(
            f"SELECT COUNT(*) FROM {schema}.mart_asset_performance_daily"
        ).fetchone()[0]
    finally:
        con.close()

    assert count > 0
