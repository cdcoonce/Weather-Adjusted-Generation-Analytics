"""Unit tests for `weather_adjusted_generation_analytics.config.settings`."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from weather_adjusted_generation_analytics.config.settings import Config


@pytest.mark.unit
def test_config_defaults_have_expected_types() -> None:
    cfg = Config()

    assert isinstance(cfg.data_raw, Path)
    assert isinstance(cfg.data_intermediate, Path)
    assert isinstance(cfg.data_processed, Path)
    assert isinstance(cfg.duckdb_path, Path)
    assert isinstance(cfg.duckdb_threads, int)

    assert cfg.log_format in {"json", "text"}


@pytest.mark.unit
def test_config_derived_paths_match_construction(tmp_path: Path) -> None:
    cfg = Config(
        data_raw=tmp_path / "raw",
        data_intermediate=tmp_path / "intermediate",
        data_processed=tmp_path / "processed",
        duckdb_path=tmp_path / "warehouse.duckdb",
        dbt_profiles_dir=tmp_path / "profiles",
        dbt_project_dir=tmp_path / "project",
        dagster_home=tmp_path / "dagster",
    )

    assert cfg.weather_raw_path == cfg.data_raw / "weather"
    assert cfg.generation_raw_path == cfg.data_raw / "generation"


@pytest.mark.unit
@pytest.mark.io
def test_ensure_directories_creates_expected_folders(temp_config: Config) -> None:
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


@pytest.mark.unit
def test_mock_asset_count_validations_enforced() -> None:
    with pytest.raises(ValidationError):
        Config(mock_asset_count=0)

    with pytest.raises(ValidationError):
        Config(mock_asset_count=101)
