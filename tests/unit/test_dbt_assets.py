"""Unit tests for the dbt_assets module.

These tests verify configuration and module structure without requiring
a Snowflake connection or running ``dbt build``.
"""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestDbtAssetsModule:
    """Verify the dbt_assets module loads and is configured correctly."""

    def test_module_imports_without_error(self) -> None:
        """The dbt_assets module can be imported."""
        from weather_analytics.assets import dbt_assets  # noqa: F401

    def test_dbt_project_dir_exists(self) -> None:
        """DBT_PROJECT_DIR points to an existing directory."""
        from weather_analytics.assets.dbt_assets import DBT_PROJECT_DIR

        assert DBT_PROJECT_DIR.is_dir(), f"Expected directory at {DBT_PROJECT_DIR}"

    def test_dbt_project_dir_contains_dbt_project_yml(self) -> None:
        """DBT_PROJECT_DIR contains a dbt_project.yml file."""
        from weather_analytics.assets.dbt_assets import DBT_PROJECT_DIR

        assert (DBT_PROJECT_DIR / "dbt_project.yml").is_file()

    def test_dbt_manifest_path_structure(self) -> None:
        """DBT_MANIFEST_PATH has the expected path structure."""
        from weather_analytics.assets.dbt_assets import DBT_MANIFEST_PATH

        assert DBT_MANIFEST_PATH.name == "manifest.json"
        assert DBT_MANIFEST_PATH.parent.name == "target"
        assert DBT_MANIFEST_PATH.parent.parent.name == "renewable_dbt"

    def test_dbt_project_dir_is_absolute(self) -> None:
        """DBT_PROJECT_DIR is an absolute path."""
        from weather_analytics.assets.dbt_assets import DBT_PROJECT_DIR

        assert DBT_PROJECT_DIR.is_absolute()

    def test_waga_dbt_assets_callable(self) -> None:
        """waga_dbt_assets is a callable (decorated dbt asset function)."""
        from weather_analytics.assets.dbt_assets import waga_dbt_assets

        assert callable(waga_dbt_assets)

    def test_profiles_dir_exists(self) -> None:
        """The profiles directory exists within the dbt project."""
        from weather_analytics.assets.dbt_assets import DBT_PROJECT_DIR

        profiles_dir = DBT_PROJECT_DIR / "profiles"
        assert profiles_dir.is_dir(), f"Expected profiles dir at {profiles_dir}"

    def test_profiles_yml_exists(self) -> None:
        """The profiles.yml file exists."""
        from weather_analytics.assets.dbt_assets import DBT_PROJECT_DIR

        profiles_yml = DBT_PROJECT_DIR / "profiles" / "profiles.yml"
        assert profiles_yml.is_file()
