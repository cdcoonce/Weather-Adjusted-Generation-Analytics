"""Dagster @dbt_assets wrapper for the renewable_dbt project.

Exposes all dbt models as Dagster software-defined assets so they
participate in the WAGA asset graph, lineage, and scheduling.
"""

from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR: Path = Path(__file__).resolve().parents[3] / "dbt" / "renewable_dbt"
DBT_MANIFEST_PATH: Path = DBT_PROJECT_DIR / "target" / "manifest.json"


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    name="waga_dbt_assets",
)
def waga_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> None:
    """Materialize all dbt models in the renewable_dbt project.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context.
    dbt : DbtCliResource
        Dagster-managed dbt CLI resource.
    """
    yield from dbt.cli(["build"], context=context).stream()
