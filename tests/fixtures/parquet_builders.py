"""Helpers for writing and reading Parquet test data.

We centralize Parquet I/O helpers so tests stay consistent and can
swap out formats later if needed.

"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq


def write_parquet(df: pl.DataFrame, path: Path) -> Path:
    """Write a Polars DataFrame to Parquet.

    Parameters
    ----------
    df : pl.DataFrame
        Data to write.
    path : Path
        Output Parquet file path.

    Returns
    -------
    Path
        The written path.

    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # NOTE:
    # We intentionally write via PyArrow instead of `polars.DataFrame.write_parquet`.
    # In some environment combinations (notably Python 3.12 + certain Polars/Arrow
    # builds), Polars' parquet sink has been observed to segfault during tests.
    # Using PyArrow's writer keeps test-data generation stable.
    table = df.to_arrow()
    pq.write_table(table, path.as_posix(), compression="snappy")
    return path


def read_parquet(path: Path) -> pl.DataFrame:
    """Read a Parquet file into a Polars DataFrame.

    Parameters
    ----------
    path : Path
        Parquet file path.

    Returns
    -------
    pl.DataFrame
        Loaded DataFrame.

    """
    return pl.read_parquet(path)
