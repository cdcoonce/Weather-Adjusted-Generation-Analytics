"""Smoke tests for test discovery and basic imports."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.config.settings import Config


@pytest.mark.unit
def test_pytest_discovers_and_imports_src(temp_config: Config) -> None:
    """Prove pytest discovery works and `src` imports resolve.

    This intentionally avoids hitting external systems (DuckDB, dlt, dbt, Dagster).
    """
    assert isinstance(temp_config, Config)


@pytest.mark.unit
def test_config_derived_paths_and_directory_creation(temp_config: Config) -> None:
    """Validate derived paths and `ensure_directories()` create expected folders."""
    assert temp_config.weather_raw_path.as_posix().endswith("/raw/weather")
    assert temp_config.generation_raw_path.as_posix().endswith("/raw/generation")

    temp_config.ensure_directories()

    expected_dirs = [
        temp_config.data_raw,
        temp_config.weather_raw_path,
        temp_config.generation_raw_path,
        temp_config.data_intermediate,
        temp_config.data_processed,
        temp_config.dbt_profiles_dir,
        temp_config.dagster_home,
    ]
    for directory in expected_dirs:
        assert directory.exists()
        assert directory.is_dir()

    assert isinstance(temp_config.duckdb_path, Path)
