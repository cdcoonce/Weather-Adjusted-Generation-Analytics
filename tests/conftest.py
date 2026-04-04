"""Pytest configuration and shared fixtures."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tests.fixtures.parquet_builders import read_parquet, write_parquet
from tests.fixtures.polars_factories import generation_df_small, weather_df_small
from weather_adjusted_generation_analytics.config.settings import Config


@pytest.fixture
def temp_config(tmp_path: Path) -> Config:
    """Create a `Config` instance rooted at a temporary directory.

    This avoids reading/writing under the repo's real `data/` directory during tests.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Config
        A configuration object with all paths set under `tmp_path`.

    """
    data_root = tmp_path / "data"
    dbt_root = tmp_path / "dbt"
    dagster_root = tmp_path / "dagster"

    return Config(
        data_raw=data_root / "raw",
        data_intermediate=data_root / "intermediate",
        data_processed=data_root / "processed",
        duckdb_path=data_root / "warehouse.duckdb",
        dbt_profiles_dir=dbt_root / "profiles",
        dbt_project_dir=dbt_root / "project",
        dagster_home=dagster_root,
    )


@pytest.fixture
def weather_df() -> pl.DataFrame:
    """Small deterministic weather DataFrame for tests."""
    return weather_df_small()


@pytest.fixture
def generation_df() -> pl.DataFrame:
    """Small deterministic generation DataFrame for tests."""
    return generation_df_small()


@pytest.fixture
def temp_parquet_dir(tmp_path: Path) -> Path:
    """Directory for parquet files written during tests."""
    return tmp_path / "parquet"


@pytest.fixture
def weather_parquet_path(temp_parquet_dir: Path, weather_df: pl.DataFrame) -> Path:
    """Write a deterministic weather parquet file to a temp directory."""
    return write_parquet(weather_df, temp_parquet_dir / "weather_2023-01-01.parquet")


@pytest.fixture
def generation_parquet_path(
    temp_parquet_dir: Path,
    generation_df: pl.DataFrame,
) -> Path:
    """Write a deterministic generation parquet file to a temp directory."""
    return write_parquet(
        generation_df,
        temp_parquet_dir / "generation_2023-01-01.parquet",
    )


@pytest.fixture
def repo_sample_data_dir() -> Path:
    """Path to committed sample data under `tests/data/`."""
    return Path(__file__).resolve().parent / "data"


@pytest.fixture
def repo_weather_sample_parquet(repo_sample_data_dir: Path) -> Path:
    """Committed weather sample parquet path (if present)."""
    return repo_sample_data_dir / "weather_2023-01-01.parquet"


@pytest.fixture
def repo_generation_sample_parquet(repo_sample_data_dir: Path) -> Path:
    """Committed generation sample parquet path (if present)."""
    return repo_sample_data_dir / "generation_2023-01-01.parquet"


@pytest.fixture
def repo_sample_weather_df(repo_weather_sample_parquet: Path) -> pl.DataFrame:
    """Load committed weather sample parquet into a DataFrame."""
    return read_parquet(repo_weather_sample_parquet)


@pytest.fixture
def repo_sample_generation_df(repo_generation_sample_parquet: Path) -> pl.DataFrame:
    """Load committed generation sample parquet into a DataFrame."""
    return read_parquet(repo_generation_sample_parquet)
