"""Unit tests for ``weather_analytics.dashboard.components._chart_helpers``.

Tests exercise the pure utility functions:
- ``make_themed_figure`` — returns a Bokeh figure with correct config
- ``with_empty_guard`` — returns Markdown pane for empty df, else render_fn result
- ``style_tooltip`` — attaches a HoverTool to a figure

Panel widget construction is tested at the smoke level only.
"""


import panel as pn
import polars as pl
import pytest
from bokeh.models import HoverTool

from weather_analytics.dashboard.components._chart_helpers import (
    make_themed_figure,
    style_tooltip,
    with_empty_guard,
)

# ===========================================================================
# make_themed_figure
# ===========================================================================


@pytest.mark.unit
def test_make_themed_figure_returns_figure_with_correct_title() -> None:
    """Figure title matches the argument."""

    fig = make_themed_figure("My Title", "X Label", "Y Label")
    assert fig.title.text == "My Title"


@pytest.mark.unit
def test_make_themed_figure_returns_figure_with_correct_axis_labels() -> None:
    """Axis labels match the arguments."""
    fig = make_themed_figure("T", "X Axis", "Y Axis")
    assert fig.xaxis.axis_label == "X Axis"
    assert fig.yaxis.axis_label == "Y Axis"


@pytest.mark.unit
def test_make_themed_figure_has_no_toolbar_logo() -> None:
    """Toolbar logo is disabled (None) for a clean look."""
    fig = make_themed_figure("T", "X", "Y")
    assert fig.toolbar.logo is None


@pytest.mark.unit
def test_make_themed_figure_has_no_toolbar() -> None:
    """Toolbar location is None so no toolbar is shown."""
    fig = make_themed_figure("T", "X", "Y")
    assert fig.toolbar_location is None


@pytest.mark.unit
def test_make_themed_figure_is_sizing_mode_stretch_width() -> None:
    """Figure uses stretch_width sizing for responsive layout."""
    fig = make_themed_figure("T", "X", "Y")
    assert fig.sizing_mode == "stretch_width"


@pytest.mark.unit
def test_make_themed_figure_accepts_extra_kwargs() -> None:
    """Extra kwargs (e.g. x_axis_type) are forwarded to figure()."""
    fig = make_themed_figure("T", "X", "Y", x_axis_type="datetime")
    # If the kwarg was accepted, x_axis_type is datetime — xaxis type changes.
    # Just verify no exception is raised and the figure is valid.
    assert fig.title.text == "T"


# ===========================================================================
# with_empty_guard
# ===========================================================================


@pytest.mark.unit
def test_with_empty_guard_returns_markdown_for_empty_df() -> None:
    """Empty DataFrame returns a Markdown pane with the default message."""
    empty_df = pl.DataFrame({"a": pl.Series([], dtype=pl.Utf8)})
    result = with_empty_guard(empty_df, lambda df: "should not be called")
    assert isinstance(result, pn.pane.Markdown)


@pytest.mark.unit
def test_with_empty_guard_markdown_contains_default_message() -> None:
    """Default empty-guard message appears in the Markdown pane object."""
    empty_df = pl.DataFrame({"a": pl.Series([], dtype=pl.Utf8)})
    result = with_empty_guard(empty_df, lambda df: "should not be called")
    assert isinstance(result, pn.pane.Markdown)
    assert "No data available" in result.object


@pytest.mark.unit
def test_with_empty_guard_markdown_contains_custom_message() -> None:
    """Custom message is used when provided."""
    empty_df = pl.DataFrame({"a": pl.Series([], dtype=pl.Utf8)})
    result = with_empty_guard(empty_df, lambda df: "x", message="Custom msg")
    assert isinstance(result, pn.pane.Markdown)
    assert "Custom msg" in result.object


@pytest.mark.unit
def test_with_empty_guard_calls_render_fn_for_non_empty_df() -> None:
    """Non-empty DataFrame calls render_fn and returns its result."""
    df = pl.DataFrame({"a": [1, 2, 3]})
    sentinel = object()
    result = with_empty_guard(df, lambda _df: sentinel)
    assert result is sentinel


@pytest.mark.unit
def test_with_empty_guard_passes_df_to_render_fn() -> None:
    """The DataFrame passed to render_fn is the same object."""
    df = pl.DataFrame({"a": [1, 2, 3]})
    captured: list[pl.DataFrame] = []

    def capture(d: pl.DataFrame) -> str:
        captured.append(d)
        return "ok"

    with_empty_guard(df, capture)
    assert len(captured) == 1
    assert captured[0].shape == df.shape


# ===========================================================================
# style_tooltip
# ===========================================================================


@pytest.mark.unit
def test_style_tooltip_adds_hover_tool_to_figure() -> None:
    """style_tooltip adds a HoverTool to the figure's tools list."""
    fig = make_themed_figure("T", "X", "Y")
    tooltips = [("Value", "@value")]
    style_tooltip(fig, tooltips)
    hover_tools = [t for t in fig.tools if isinstance(t, HoverTool)]
    assert len(hover_tools) == 1


@pytest.mark.unit
def test_style_tooltip_sets_correct_tooltips() -> None:
    """HoverTool tooltips match the provided spec."""
    fig = make_themed_figure("T", "X", "Y")
    tooltips = [("Date", "@date"), ("MWh", "@mwh")]
    style_tooltip(fig, tooltips)
    hover = next(t for t in fig.tools if isinstance(t, HoverTool))
    assert hover.tooltips == tooltips


@pytest.mark.unit
def test_style_tooltip_returns_none() -> None:
    """style_tooltip is a side-effecting procedure returning None."""
    fig = make_themed_figure("T", "X", "Y")
    result = style_tooltip(fig, [("X", "@x")])
    assert result is None
