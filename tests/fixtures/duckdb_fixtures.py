"""DuckDB helpers for tests.

These helpers are designed to keep DuckDB-backed tests small, deterministic,
and easy to reason about.

"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
import polars as pl


@dataclass(frozen=True, slots=True)
class DuckDBDatasetSpec:
    """Specification for dataset/schema naming in DuckDB."""

    schema: str = "renewable_energy"
    weather_table: str = "weather"
    generation_table: str = "generation"


def create_schema(conn: duckdb.DuckDBPyConnection, schema: str) -> None:
    """Create a schema if it does not exist."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def load_polars_table(
    *,
    conn: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
    schema: str,
    table: str,
) -> None:
    """Load a Polars DataFrame into DuckDB as a table.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection.
    df : pl.DataFrame
        Data to load.
    schema : str
        Schema name.
    table : str
        Table name.

    Returns
    -------
    None

    """
    create_schema(conn, schema)

    # Register via Arrow/Polars bridge and materialize as a physical table.
    relation_name = f"_{schema}_{table}_df"
    conn.register(relation_name, df.to_arrow())
    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    conn.execute(
        f"CREATE TABLE {schema}.{table} AS SELECT * FROM {relation_name}"
    )
    conn.unregister(relation_name)


def load_weather_and_generation(
    *,
    conn: duckdb.DuckDBPyConnection,
    weather_df: pl.DataFrame,
    generation_df: pl.DataFrame,
    spec: DuckDBDatasetSpec | None = None,
) -> DuckDBDatasetSpec:
    """Create schema and load weather + generation tables.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection.
    weather_df : pl.DataFrame
        Weather dataset.
    generation_df : pl.DataFrame
        Generation dataset.
    spec : DuckDBDatasetSpec | None, default=None
        Schema/table naming spec.

    Returns
    -------
    DuckDBDatasetSpec
        The spec used.

    """
    if spec is None:
        spec = DuckDBDatasetSpec()

    load_polars_table(
        conn=conn,
        df=weather_df,
        schema=spec.schema,
        table=spec.weather_table,
    )
    load_polars_table(
        conn=conn,
        df=generation_df,
        schema=spec.schema,
        table=spec.generation_table,
    )

    return spec
