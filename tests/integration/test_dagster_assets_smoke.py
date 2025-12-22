"""Phase 5 Dagster integration smoke tests.

We materialize selected assets in-process against a temp DuckDB.

"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest
from dagster import materialize

from dags.dagster_project.assets import correlation_asset
from tests.fixtures.duckdb_fixtures import load_weather_and_generation


def _weather_df_for_corr() -> pl.DataFrame:
    rows = [
        {
            "timestamp": datetime(2023, 1, 1, 0, 0, 0),
            "asset_id": "asset_001",
            "wind_speed_mps": 5.0,
            "ghi": 200.0,
        },
        {
            "timestamp": datetime(2023, 1, 1, 1, 0, 0),
            "asset_id": "asset_001",
            "wind_speed_mps": 6.0,
            "ghi": 250.0,
        },
    ]
    return pl.DataFrame(rows).with_columns(
        [
            pl.col("timestamp").cast(pl.Datetime),
            pl.col("asset_id").cast(pl.Utf8),
        ]
    )


def _generation_df_for_corr() -> pl.DataFrame:
    rows = [
        {
            "timestamp": datetime(2023, 1, 1, 0, 0, 0),
            "asset_id": "asset_001",
            "net_generation_mwh": 50.0,
            "asset_capacity_mw": 100.0,
        },
        {
            "timestamp": datetime(2023, 1, 1, 1, 0, 0),
            "asset_id": "asset_001",
            "net_generation_mwh": 51.0,
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
@pytest.mark.dagster
@pytest.mark.duckdb
@pytest.mark.io
def test_materialize_correlation_asset_against_temp_duckdb(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    duckdb_path = tmp_path / "warehouse.duckdb"

    con = duckdb.connect(str(duckdb_path))
    try:
        # correlation_asset expects both weather and generation tables in schema
        load_weather_and_generation(
            conn=con,
            weather_df=_weather_df_for_corr(),
            generation_df=_generation_df_for_corr(),
        )
    finally:
        con.close()

    # Patch the module-level config used by the Dagster assets module.
    import dags.dagster_project.assets as assets_module

    monkeypatch.setattr(
        assets_module,
        "config",
        SimpleNamespace(duckdb_path=duckdb_path, dlt_schema="renewable_energy"),
    )

    result = materialize([correlation_asset])

    assert result.success
    output = result.output_for_node("weather_generation_correlation")
    assert isinstance(output, dict)
    assert output["total_records"] > 0
    assert isinstance(output["correlations"], list)
    assert len(output["correlations"]) >= 1
