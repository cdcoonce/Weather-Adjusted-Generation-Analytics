"""Reactive filter state for the WAGA dashboard.

Exports a ``Filters`` ``param.Parameterized`` class whose params drive all
reactive dashboard components, and a pure ``filter_assets_by_type()`` helper
that is tested independently.

**Bundler note**: this file is inlined by ``scripts/build_dashboard_app.py``
before ``app.py`` is appended. Imports from ``weather_analytics.dashboard.*``
are stripped automatically. All other imports must be available inside Pyodide
or deferred inside functions.
"""

from typing import Any

import param
import polars as pl


def filter_assets_by_type(assets_df: pl.DataFrame, asset_type: str) -> list[str]:
    """Return a list of asset_id values matching *asset_type*, prefixed by 'All'.

    Parameters
    ----------
    assets_df : pl.DataFrame
        Assets table with at minimum ``asset_id`` and ``asset_type`` columns.
    asset_type : str
        One of ``"All"``, ``"Wind"``, or ``"Solar"``. When ``"All"``, every
        asset_id is returned regardless of type.

    Returns
    -------
    list[str]
        ``["All", *matching_asset_ids]``. Always starts with ``"All"`` so the
        param Selector always has a valid default.
    """
    if assets_df.is_empty():
        return ["All"]
    if asset_type == "All":
        ids = assets_df["asset_id"].to_list()
    else:
        ids = assets_df.filter(
            pl.col("asset_type").str.to_lowercase() == asset_type.lower()
        )["asset_id"].to_list()
    return ["All", *ids]


class Filters(param.Parameterized):
    """Reactive parameter container for dashboard-wide filter state.

    Params
    ------
    asset_id : str
        Currently selected asset. ``"All"`` means no asset filter applied.
    asset_type : str
        Asset-type toggle: ``"All"``, ``"Wind"``, or ``"Solar"``.
    date_start : str
        ISO-8601 date string for the start of the selected range (inclusive).
        Empty string means no lower bound.
    date_end : str
        ISO-8601 date string for the end of the selected range (inclusive).
        Empty string means no upper bound.
    """

    asset_id: Any = param.Selector(default="All", objects=["All"])
    asset_type: Any = param.Selector(default="All", objects=["All", "Wind", "Solar"])
    date_start: Any = param.String(default="")
    date_end: Any = param.String(default="")

    # Internal store so the asset_type watcher can re-filter.
    _assets_df: pl.DataFrame = param.Parameter(default=None, precedence=-1)

    def initialize(
        self, assets_df: pl.DataFrame, date_start: str, date_end: str
    ) -> None:
        """Populate filter state from loaded data.

        Call this once after awaiting the data loaders. Sets the date strings
        and populates ``asset_id.objects`` with every asset in *assets_df*.

        Parameters
        ----------
        assets_df : pl.DataFrame
            Assets table (``asset_id``, ``asset_type``, ``display_name``, …).
        date_start : str
            ISO-8601 start date from the manifest (e.g. ``"2025-01-01"``).
        date_end : str
            ISO-8601 end date from the manifest (e.g. ``"2026-04-11"``).
        """
        self._assets_df = assets_df
        self.date_start = date_start
        self.date_end = date_end
        objects = filter_assets_by_type(assets_df, "All")
        self.param["asset_id"].objects = objects
        # Keep asset_id at "All" after populating the list.
        self.asset_id = "All"

    @param.depends("asset_type", watch=True)
    def _reset_asset_id_on_type_change(self) -> None:
        """Watcher: re-filter asset objects and reset asset_id if needed."""
        assets_df = self._assets_df
        if assets_df is None:
            return
        new_objects = filter_assets_by_type(assets_df, self.asset_type)
        self.param["asset_id"].objects = new_objects
        if self.asset_id not in new_objects:
            self.asset_id = "All"
