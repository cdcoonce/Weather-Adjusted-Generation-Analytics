"""Phase 5 integration smoke tests for dbt.

These tests validate that dbt can build a small subset of the project
against an isolated DuckDB database.

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
    """Create a tiny, dbt-compatible weather dataset."""

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
            "wind_speed_mps": 5.5,
            "wind_direction_deg": 190.0,
            "ghi": 210.0,
            "temperature_c": 15.5,
            "pressure_hpa": 1013.00,
            "relative_humidity": 49.0,
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
    """Create a tiny, dbt-compatible generation dataset."""

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
def test_dbt_build_staging_models_against_temp_duckdb(tmp_path: Path) -> None:
    project_dir = Path(__file__).resolve().parents[2] / "dbt" / "renewable_dbt"

    # Skip cleanly if dbt packages are missing (we avoid mutating the repo in tests).
    if not (project_dir / "dbt_packages" / "dbt_utils").exists():
        pytest.skip("dbt packages missing; run `uv run dbt deps` in dbt/renewable_dbt")

    duckdb_path = tmp_path / "warehouse.duckdb"
    profiles_dir = tmp_path / "dbt_profiles"
    target_path = tmp_path / "dbt_target"

    write_temp_profiles_yml(profiles_dir=profiles_dir, duckdb_path=duckdb_path)

    # Seed raw tables expected by dbt sources.
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

    run_dbt(
        "build",
        "--select",
        "staging.stg_weather",
        "staging.stg_generation",
        invocation=invocation,
    )

    # Assert the staging views exist.
    con = duckdb.connect(str(duckdb_path))
    try:
        relations = con.execute(
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_name IN ('stg_weather', 'stg_generation')"
        ).fetchall()
    finally:
        con.close()

    assert any(name == "stg_weather" for _, name in relations)
    assert any(name == "stg_generation" for _, name in relations)
