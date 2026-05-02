"""Bokeh theme for the WAGA dashboard matching charleslikesdata.com.

Exports a ``portfolio_theme`` object that is applied globally inside
``app.py``'s ``servable()`` function — NOT at module import time. Applying
at import time mutates Panel's module-level config and causes cross-test
pollution. If you find yourself importing this module and immediately
setting ``pn.config.theme``, move that call into the servable function.
"""

from typing import Any

# Palette derived from charleslikesdata.com style.css. Keep in sync with
# ``static/portfolio.css``; if the portfolio palette changes, update both.
PORTFOLIO_PALETTE: dict[str, str] = {
    "text_primary": "#353535",
    "text_secondary": "#555555",
    "bg_white": "#ffffff",
    "bg_light": "#f9f9f9",
    "bg_card_highlight": "#ebf1f8",
    "border": "#353535",
    "grid": "#f0f0f0",
}

# Data palette — desaturated, editorial. Used to encode data, not decorate.
DATA_PALETTE: dict[str, str] = {
    "primary": "#353535",
    "secondary": "#a8b8c8",
    "wind": "#4a7c7e",
    "solar": "#d4a44c",
    "performance_excellent": "#4a7c5c",
    "performance_good": "#7ca087",
    "performance_fair": "#c8a472",
    "performance_poor": "#b06a5c",
    "rolling": "#888888",
    "expected_ghost": "#cccccc",
}


def build_theme_json() -> dict[str, Any]:
    """Return the Bokeh theme JSON config.

    The theme applies Poppins font, charcoal axes, subtle dashed gridlines,
    and removes the Bokeh logo and toolbar chrome.

    Returns
    -------
    dict[str, Any]
        JSON dict suitable for constructing a ``bokeh.themes.Theme``.
    """
    return {
        "attrs": {
            "figure": {
                "background_fill_color": PORTFOLIO_PALETTE["bg_white"],
                "border_fill_color": PORTFOLIO_PALETTE["bg_white"],
                "outline_line_color": None,
            },
            "Axis": {
                "axis_label_text_font": "Poppins",
                "axis_label_text_font_size": "12px",
                "axis_label_text_color": PORTFOLIO_PALETTE["text_primary"],
                "axis_label_text_font_style": "normal",
                "major_label_text_font": "Poppins",
                "major_label_text_color": PORTFOLIO_PALETTE["text_secondary"],
                "major_tick_line_color": PORTFOLIO_PALETTE["text_primary"],
                "minor_tick_line_color": None,
                "axis_line_color": PORTFOLIO_PALETTE["text_primary"],
            },
            "Grid": {
                "grid_line_color": PORTFOLIO_PALETTE["grid"],
                "grid_line_dash": [4, 4],
            },
            "Title": {
                "text_font": "Poppins",
                "text_font_size": "14px",
                "text_font_style": "normal",
                "text_color": PORTFOLIO_PALETTE["text_primary"],
            },
            "Legend": {
                "background_fill_alpha": 0.9,
                "border_line_color": None,
                "label_text_font": "Poppins",
                "label_text_color": PORTFOLIO_PALETTE["text_secondary"],
            },
            "Toolbar": {
                "logo": None,
            },
        }
    }


def load_theme() -> Any:
    """Construct the Bokeh ``Theme`` object.

    Import is lazy so tests and non-Pyodide contexts can import this
    module without requiring Bokeh to be installed.

    Returns
    -------
    bokeh.themes.Theme
        Theme object suitable for ``pn.config.theme =``.
    """
    from bokeh.themes import Theme

    return Theme(json=build_theme_json())
