"""Unit tests for the dbt_assets module.

These tests verify configuration and module structure without requiring
a Snowflake connection or running ``dbt build``.
"""

from __future__ import annotations

import pytest

from weather_analytics.assets.dbt_assets import (
    DBT_MANIFEST_PATH,
    DBT_PROJECT_DIR,
    waga_dbt_assets,
)

_manifest_missing = not DBT_MANIFEST_PATH.exists()


@pytest.mark.unit
class TestDbtAssetsModule:
    """Verify the dbt_assets module loads and is configured correctly."""

    def test_module_imports_without_error(self) -> None:
        """The dbt_assets module can be imported."""
        from weather_analytics.assets import dbt_assets  # noqa: F401

    def test_dbt_project_dir_exists(self) -> None:
        """DBT_PROJECT_DIR points to an existing directory."""
        assert DBT_PROJECT_DIR.is_dir(), f"Expected directory at {DBT_PROJECT_DIR}"

    def test_dbt_project_dir_contains_dbt_project_yml(self) -> None:
        """DBT_PROJECT_DIR contains a dbt_project.yml file."""
        assert (DBT_PROJECT_DIR / "dbt_project.yml").is_file()

    def test_dbt_manifest_path_structure(self) -> None:
        """DBT_MANIFEST_PATH has the expected path structure."""
        assert DBT_MANIFEST_PATH.name == "manifest.json"
        assert DBT_MANIFEST_PATH.parent.name == "target"
        assert DBT_MANIFEST_PATH.parent.parent.name == "renewable_dbt"

    def test_dbt_project_dir_is_absolute(self) -> None:
        """DBT_PROJECT_DIR is an absolute path."""
        assert DBT_PROJECT_DIR.is_absolute()

    @pytest.mark.skipif(
        _manifest_missing,
        reason="dbt manifest.json not generated (run dbt parse)",
    )
    def test_waga_dbt_assets_callable(self) -> None:
        """waga_dbt_assets is a callable when manifest exists."""
        assert callable(waga_dbt_assets)

    def test_profiles_dir_exists(self) -> None:
        """The profiles directory exists within the dbt project."""
        profiles_dir = DBT_PROJECT_DIR / "profiles"
        assert profiles_dir.is_dir(), f"Expected profiles dir at {profiles_dir}"

    def test_profiles_yml_exists(self) -> None:
        """The profiles.yml file exists."""
        profiles_yml = DBT_PROJECT_DIR / "profiles" / "profiles.yml"
        assert profiles_yml.is_file()

    @pytest.mark.skipif(
        _manifest_missing,
        reason="dbt manifest.json not generated (run dbt parse)",
    )
    def test_waga_dbt_assets_is_none_without_manifest(self) -> None:
        """When manifest exists, waga_dbt_assets is not None."""
        assert waga_dbt_assets is not None
