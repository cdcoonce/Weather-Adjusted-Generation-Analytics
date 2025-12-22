"""Phase 2 fixture validation tests.

These tests ensure the shared fixtures behave as expected and remain
stable building blocks for later phases.

"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
@pytest.mark.io
def test_parquet_roundtrip_paths_exist(
    weather_parquet_path: Path,
    generation_parquet_path: Path,
) -> None:
    """Verify parquet writers create files in temp directories."""
    assert weather_parquet_path.exists()
    assert generation_parquet_path.exists()


@pytest.mark.unit
@pytest.mark.duckdb
def test_duckdb_join_smoke(
    duckdb_conn_in_memory,
    duckdb_loaded_tables,
) -> None:
    """Load two tables into DuckDB and validate a simple join works."""
    spec = duckdb_loaded_tables

    row_count = duckdb_conn_in_memory.execute(
        f"""
        SELECT COUNT(*)
        FROM {spec.schema}.{spec.weather_table} w
        INNER JOIN {spec.schema}.{spec.generation_table} g
            ON w.asset_id = g.asset_id
            AND w.timestamp = g.timestamp
        """
    ).fetchone()[0]

    assert row_count > 0
