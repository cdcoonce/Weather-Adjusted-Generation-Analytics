"""Tests that every asset check is attached to a real, materializable asset.

Sibling bug to the one in ``test_asset_dependencies.py``, same root cause:
``waga_dbt_assets`` is the *op* / ``AssetsDefinition`` name, not an asset key.
dagster-dbt keys each dbt model by its folder path relative to the models
directory (``AssetKey(["staging", "stg_weather"])``,
``AssetKey(["marts", "mart_asset_performance_daily"])``, ...), so
``@asset_check(asset=AssetKey(["waga_dbt_assets"]))`` attaches the check to a
phantom key that nothing materializes.

Dagster does not reject that — the check loads, appears in the Definitions,
and is simply never selected by a job that selects real assets. In production
this silently cost five of the eight custom checks: the first server run of
``waga_daily_job`` (2026-09-14) executed only the two raw row-count checks,
because those were the only two pointed at keys the job actually contained.

The first class holds without a dbt manifest (a ``CheckSpec``'s target key is
static on the decorated object, independent of whether the dbt assets are
built) so it runs in CI too. The second class needs the real manifest to
build the full asset graph, and is skipped when it is absent.
"""

from __future__ import annotations

import pytest
from dagster import AssetKey

from weather_analytics.assets.dbt_assets import dbt_project
from weather_analytics.definitions import defs

_manifest_missing = not dbt_project.manifest_path.exists()

# The op/AssetsDefinition name, which is *not* an asset key. No check may
# target it.
_DBT_OP_NAME_KEY = AssetKey(["waga_dbt_assets"])

# Every custom check, mapped to the asset that produces the table its SQL
# reads (see ``checks/data_quality.py``):
#   WAGA.STAGING.stg_weather              -> staging/stg_weather
#   WAGA.STAGING.stg_generation           -> staging/stg_generation
#   WAGA.MARTS.mart_asset_performance_daily -> marts/mart_asset_performance_daily
#   WAGA.RAW.weather / .generation        -> the dlt ingestion assets
#   WAGA.ANALYTICS.correlation_results    -> waga_correlation_analysis
_EXPECTED_CHECK_TARGETS = {
    "waga_weather_freshness_check": AssetKey(["staging", "stg_weather"]),
    "waga_generation_freshness_check": AssetKey(["staging", "stg_generation"]),
    "waga_weather_value_range_check": AssetKey(["staging", "stg_weather"]),
    "waga_generation_value_range_check": AssetKey(["staging", "stg_generation"]),
    "waga_mart_performance_row_count_check": AssetKey(
        ["marts", "mart_asset_performance_daily"]
    ),
    "waga_raw_weather_row_count_check": AssetKey(["waga_weather_ingestion"]),
    "waga_raw_generation_row_count_check": AssetKey(["waga_generation_ingestion"]),
    "waga_mart_correlation_row_count_check": AssetKey(["waga_correlation_analysis"]),
}

# The five checks that read dbt-produced tables. These are the ones the
# phantom-key bug dropped from ``waga_daily_job``.
_DBT_BACKED_CHECK_NAMES = frozenset(
    {
        "waga_weather_freshness_check",
        "waga_generation_freshness_check",
        "waga_weather_value_range_check",
        "waga_generation_value_range_check",
        "waga_mart_performance_row_count_check",
    }
)


def _declared_check_targets() -> dict[str, AssetKey]:
    """Map every custom check's name to the asset key it is attached to."""
    return {
        spec.name: spec.asset_key
        for check_def in defs.asset_checks or []
        for spec in check_def.check_specs
    }


@pytest.mark.unit
class TestCheckTargetsWithoutManifest:
    """A check's target key is static on the decorated object — no manifest
    needed to inspect it, so these run in CI too."""

    def test_no_check_targets_the_dbt_op_name(self) -> None:
        """``waga_dbt_assets`` names the op, not an asset. A check pointed at
        it attaches to a phantom key and never runs."""
        offenders = sorted(
            name
            for name, key in _declared_check_targets().items()
            if key == _DBT_OP_NAME_KEY
        )
        assert offenders == [], (
            "asset checks targeting the non-asset key "
            f"{_DBT_OP_NAME_KEY.to_user_string()!r}: {offenders}. dbt models are "
            "keyed by folder path (e.g. staging/stg_weather), not by the "
            "AssetsDefinition name."
        )

    def test_each_check_targets_the_asset_producing_the_table_it_reads(self) -> None:
        """Pin the full check -> target mapping against the table each check's
        SQL actually queries."""
        assert _declared_check_targets() == _EXPECTED_CHECK_TARGETS


@pytest.mark.unit
@pytest.mark.skipif(
    _manifest_missing,
    reason="dbt manifest.json not generated (run dbt parse)",
)
class TestCheckTargetsInAssetGraph:
    """Graph-level checks that need the real dbt manifest — dbt models only
    appear in the asset graph once a manifest exists."""

    def test_every_asset_check_targets_a_materializable_asset(self) -> None:
        """Every check in the graph — custom *and* dbt-generated — must be
        attached to an asset something actually materializes.

        This is the general invariant behind the bug: a check on a
        non-materializable key is dead weight that no asset-selecting job
        will ever run.
        """
        graph = defs.resolve_asset_graph()
        materializable = graph.materializable_asset_keys

        orphaned = sorted(
            f"{check_key.asset_key.to_user_string()}::{check_key.name}"
            for check_key in graph.asset_check_keys
            if check_key.asset_key not in materializable
        )
        assert orphaned == [], (
            "asset checks attached to non-materializable keys (they will "
            f"never be selected by an asset job): {orphaned}"
        )

    def test_daily_job_runs_the_dbt_backed_checks(self) -> None:
        """The five checks that read dbt-produced tables must be selected by
        ``waga_daily_job``.

        ``waga_daily_job`` selects ``AssetSelection.groups("default")``, which
        covers every dbt model, so a correctly-targeted check rides along.
        With the phantom key these five were silently absent from the run.
        """
        job = defs.resolve_job_def("waga_daily_job")
        selected = {
            check_key.name for check_key in job.asset_layer.asset_graph.asset_check_keys
        }

        missing = sorted(_DBT_BACKED_CHECK_NAMES - selected)
        assert missing == [], (
            f"checks missing from waga_daily_job: {missing}. Selected waga_* "
            f"checks: {sorted(n for n in selected if n.startswith('waga_'))}"
        )

    def test_daily_job_check_targets_match_declarations(self) -> None:
        """Each dbt-backed check is attached, in the resolved job, to the dbt
        key its declaration names — proving the retarget survived graph
        resolution rather than landing on another stand-in."""
        job = defs.resolve_job_def("waga_daily_job")
        resolved = {
            check_key.name: check_key.asset_key
            for check_key in job.asset_layer.asset_graph.asset_check_keys
        }

        expected = {
            name: key
            for name, key in _EXPECTED_CHECK_TARGETS.items()
            if name in _DBT_BACKED_CHECK_NAMES
        }
        actual = {name: resolved.get(name) for name in expected}
        assert actual == expected
