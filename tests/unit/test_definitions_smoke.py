"""Smoke tests for the weather_analytics Definitions object."""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestDefinitionsSmoke:
    """Verify the Dagster Definitions object loads without error."""

    def test_definitions_import(self) -> None:
        """Definitions module can be imported."""
        from weather_analytics.definitions import defs  # noqa: F401

    def test_definitions_is_definitions_instance(self) -> None:
        """defs is a Dagster Definitions instance."""
        from dagster import Definitions
        from weather_analytics.definitions import defs

        assert isinstance(defs, Definitions)

    def test_definitions_has_resources(self) -> None:
        """Definitions includes at least the Snowflake resource."""
        from weather_analytics.definitions import defs

        resources = defs.resources
        assert resources is not None
        assert "snowflake" in resources

    def test_package_importable(self) -> None:
        """The weather_analytics package itself can be imported."""
        import weather_analytics  # noqa: F401
