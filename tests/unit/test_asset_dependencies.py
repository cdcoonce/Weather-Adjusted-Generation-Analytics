"""Tests for dbt-mart dependency wiring on non-dbt assets.

dagster-dbt keys each dbt model by its folder path relative to the models
directory (e.g. ``AssetKey(["marts", "mart_asset_performance_daily"])``), not
by the bare model name. A ``deps=["mart_asset_performance_daily"]`` string
looks plausible but resolves to a *different*, non-materializable stand-in
key that dagster silently creates for the reference — it imposes no
execution-order constraint against the real dbt asset.

This bit in production: ``waga_dashboard_export_build`` read the marts
before ``waga_dbt_assets`` rebuilt them, because its ``deps=`` pointed at
phantom keys instead of the dbt-produced ``marts/...`` keys. See
``dashboard_export.py`` and ``correlation.py``.

The first test class holds without a dbt manifest (CI has none — the
declared dependency keys on the ``AssetsDefinition`` are static regardless of
whether the dbt assets are built). The second class needs the real manifest
to build the full asset graph and is skipped when it is absent.
"""

from __future__ import annotations

import pytest
from dagster import AssetKey

from weather_analytics.assets.analytics.correlation import waga_correlation_analysis
from weather_analytics.assets.analytics.dashboard_export import (
    waga_dashboard_export_build,
)
from weather_analytics.assets.dbt_assets import dbt_project
from weather_analytics.assets.dbt_assets import waga_dbt_assets as dbt_assets_def
from weather_analytics.definitions import defs

_manifest_missing = not dbt_project.manifest_path.exists()

_EXPECTED_EXPORT_DEPS = {
    AssetKey(["marts", "mart_asset_performance_daily"]),
    AssetKey(["marts", "mart_asset_weather_performance"]),
    AssetKey(["marts", "dim_asset"]),
}
_EXPECTED_CORRELATION_DEPS = {
    AssetKey(["marts", "mart_asset_performance_daily"]),
}


@pytest.mark.unit
class TestDeclaredDepKeysWithoutManifest:
    """Declared dep keys are static on the ``AssetsDefinition`` — no manifest
    needed to inspect them, so these run in CI too."""

    def test_dashboard_export_deps_use_prefixed_mart_keys(self) -> None:
        """``waga_dashboard_export_build`` must depend on the real,
        folder-prefixed dbt mart keys, not bare model-name stand-ins."""
        assert waga_dashboard_export_build.dependency_keys == _EXPECTED_EXPORT_DEPS

    def test_correlation_deps_use_prefixed_mart_keys(self) -> None:
        """``waga_correlation_analysis`` must depend on the real,
        folder-prefixed dbt mart key, not a bare model-name stand-in."""
        assert waga_correlation_analysis.dependency_keys == _EXPECTED_CORRELATION_DEPS


@pytest.mark.unit
@pytest.mark.skipif(
    _manifest_missing,
    reason="dbt manifest.json not generated (run dbt parse)",
)
class TestAssetGraphWithManifest:
    """Dangling-dependency and ordering checks that need the real dbt
    manifest to build the full asset graph (dbt models only appear in the
    graph once a manifest exists)."""

    def test_no_dangling_deps_among_non_dbt_assets(self) -> None:
        """Every parent key of every non-dbt asset must be materializable.

        A dep pointed at a bare, un-prefixed dbt model name resolves to a
        phantom stand-in key that dagster creates for the unresolved
        reference; that stand-in is never materializable. This is the exact
        shape of the production bug: it would have failed on the old string
        deps in ``dashboard_export.py`` / ``correlation.py``.
        """
        graph = defs.resolve_asset_graph()
        dbt_keys = dbt_assets_def.keys
        materializable = graph.materializable_asset_keys
        non_dbt_keys = [key for key in materializable if key not in dbt_keys]

        assert non_dbt_keys, "expected at least one non-dbt materializable asset"

        dangling = {
            (key, parent)
            for key in non_dbt_keys
            for parent in graph.get(key).parent_keys
            if parent not in materializable
        }
        assert dangling == set(), (
            "non-dbt assets with a dangling (non-materializable) parent dep: "
            f"{dangling}"
        )

    def test_export_is_downstream_of_dbt_marts_in_asset_graph(self) -> None:
        """``waga_dashboard_export_build``'s ancestor closure must include
        the real, dbt-produced mart key — proving the export is ordered
        after dbt in the asset graph, not merely coincidentally adjacent."""
        graph = defs.resolve_asset_graph()
        export_key = AssetKey(["waga_dashboard_export_build"])
        ancestors = graph.get_ancestor_asset_keys(export_key)

        real_mart_key = AssetKey(["marts", "mart_asset_performance_daily"])
        assert real_mart_key in ancestors, (
            f"expected {real_mart_key} in the ancestor closure of {export_key}, "
            f"got: {ancestors}"
        )

    def test_export_op_is_topologically_after_dbt_op_in_daily_job(self) -> None:
        """In ``waga_daily_job``'s resolved execution plan, the dbt step
        must precede the dashboard-export step — a topological sort places
        a dependency before its dependent whenever a real edge connects
        them, so this fails if the edge is missing (as it was in production,
        where the export read stale marts because it ran before dbt)."""
        job = defs.resolve_job_def("waga_daily_job")
        order = [node.name for node in job.nodes_in_topological_order]

        dbt_index = order.index("waga_dbt_assets")
        export_index = order.index("waga_dashboard_export_build")
        assert dbt_index < export_index, (
            "waga_dbt_assets must precede waga_dashboard_export_build in "
            f"waga_daily_job's topological order, got: {order}"
        )
