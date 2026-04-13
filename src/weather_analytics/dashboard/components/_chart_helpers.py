"""Shared Bokeh figure utilities for the WAGA dashboard.

Exports:
- ``make_themed_figure`` — factory for themed, toolbar-free Bokeh figures
- ``with_empty_guard`` — renders a Markdown placeholder for empty DataFrames
- ``style_tooltip`` — attaches a HoverTool to a figure

**Bundler note**: this file is inlined by ``scripts/build_dashboard_app.py``
before ``app.py`` is appended. Imports from ``weather_analytics.dashboard.*``
are stripped automatically.
"""

from typing import Any

import polars as pl
from bokeh.plotting import figure


def make_themed_figure(
    title: str,
    x_label: str,
    y_label: str,
    **kwargs: Any,
) -> Any:
    """Create a themed Bokeh figure with no toolbar and stretch_width sizing.

    Parameters
    ----------
    title : str
        Chart title displayed above the plot.
    x_label : str
        Label for the x-axis.
    y_label : str
        Label for the y-axis.
    **kwargs : Any
        Additional keyword arguments forwarded to ``bokeh.plotting.figure``
        (e.g. ``x_axis_type="datetime"``).

    Returns
    -------
    bokeh.plotting.figure
        Configured Bokeh figure ready for glyphs.
    """
    fig = figure(
        title=title,
        x_axis_label=x_label,
        y_axis_label=y_label,
        toolbar_location=None,
        sizing_mode="stretch_width",
        **kwargs,
    )
    fig.toolbar.logo = None
    return fig


def with_empty_guard(
    df: pl.DataFrame,
    render_fn: Any,
    message: str = "No data available",
) -> Any:
    """Return a Markdown placeholder when *df* is empty, else call *render_fn*.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to check.
    render_fn : callable
        Called with *df* when it is non-empty. Its return value is passed
        through to the caller.
    message : str, optional
        Message shown inside the Markdown pane when *df* is empty.
        Defaults to ``"No data available"``.

    Returns
    -------
    Any
        ``pn.pane.Markdown`` when *df* is empty, else ``render_fn(df)``.
    """
    import panel as pn

    if df.is_empty():
        return pn.pane.Markdown(f"_{message}_")
    return render_fn(df)


def style_tooltip(fig: Any, tooltips: list[tuple[str, str]]) -> None:
    """Attach a HoverTool with *tooltips* to *fig*.

    Parameters
    ----------
    fig : bokeh.plotting.figure
        Target Bokeh figure.
    tooltips : list[tuple[str, str]]
        Tooltip spec as ``[(label, "@column"), ...]``.

    Returns
    -------
    None
    """
    from bokeh.models import HoverTool

    hover = HoverTool(tooltips=tooltips)
    fig.add_tools(hover)
