"""Dagster @dbt_assets wrapper for the renewable_dbt project.

Exposes all dbt models as Dagster software-defined assets so they
participate in the WAGA asset graph, lineage, and scheduling.

The manifest is generated at build time (``dbt parse`` in the Dagster
Cloud build step).  When running locally without a manifest (e.g. in
CI unit tests), the module still imports — but ``waga_dbt_assets`` will
be ``None`` and must be excluded from ``Definitions``.
"""

from pathlib import Path
from typing import Any

DBT_PROJECT_DIR: Path = Path(__file__).resolve().parents[3] / "dbt" / "renewable_dbt"
DBT_MANIFEST_PATH: Path = DBT_PROJECT_DIR / "target" / "manifest.json"

waga_dbt_assets: Any = None

if DBT_MANIFEST_PATH.exists():
    from dagster import AssetExecutionContext
    from dagster_dbt import DbtCliResource, dbt_assets

    @dbt_assets(
        manifest=DBT_MANIFEST_PATH,
        name="waga_dbt_assets",
    )
    def waga_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> None:  # type: ignore[no-redef]
        """Materialize all dbt models in the renewable_dbt project.

        Parameters
        ----------
        context : AssetExecutionContext
            Dagster execution context.
        dbt : DbtCliResource
            Dagster-managed dbt CLI resource.
        """
        yield from dbt.cli(["build"], context=context).stream()
