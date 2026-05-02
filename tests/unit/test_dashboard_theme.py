"""Unit tests for ``weather_analytics.dashboard.theme``."""


import pytest

from weather_analytics.dashboard.theme import (
    DATA_PALETTE,
    PORTFOLIO_PALETTE,
    build_theme_json,
)


@pytest.mark.unit
def test_portfolio_palette_contains_required_keys() -> None:
    """Palette must expose the keys used by the Bokeh theme and CSS."""
    required = {
        "text_primary",
        "text_secondary",
        "bg_white",
        "bg_light",
        "bg_card_highlight",
        "border",
        "grid",
    }
    assert required.issubset(PORTFOLIO_PALETTE.keys())


@pytest.mark.unit
def test_portfolio_palette_matches_charleslikesdata() -> None:
    """Guard: these exact hex values come from charleslikesdata.com style.css.

    If the portfolio site's palette changes, update both this test AND
    the CSS file in static/portfolio.css to match.
    """
    assert PORTFOLIO_PALETTE["text_primary"] == "#353535"
    assert PORTFOLIO_PALETTE["bg_white"] == "#ffffff"
    assert PORTFOLIO_PALETTE["bg_light"] == "#f9f9f9"


@pytest.mark.unit
def test_data_palette_has_wind_and_solar_colors() -> None:
    """Charts color-encode wind vs solar assets — those keys must exist."""
    assert "wind" in DATA_PALETTE
    assert "solar" in DATA_PALETTE
    assert DATA_PALETTE["wind"].startswith("#")
    assert DATA_PALETTE["solar"].startswith("#")


@pytest.mark.unit
def test_data_palette_has_performance_categories() -> None:
    """Performance distribution stacked bar needs four categories."""
    for key in (
        "performance_excellent",
        "performance_good",
        "performance_fair",
        "performance_poor",
    ):
        assert key in DATA_PALETTE


@pytest.mark.unit
def test_build_theme_json_shape() -> None:
    """Bokeh Theme expects a dict with an ``attrs`` key mapping model names
    to property overrides. Verify the high-level shape without loading bokeh."""
    theme_json = build_theme_json()
    assert "attrs" in theme_json
    attrs = theme_json["attrs"]
    for model in ("figure", "Axis", "Grid", "Title", "Legend", "Toolbar"):
        assert model in attrs, f"missing theme overrides for {model}"


@pytest.mark.unit
def test_theme_hides_toolbar_logo() -> None:
    """Minimal-styling default: no Bokeh logo on the toolbar."""
    theme_json = build_theme_json()
    assert theme_json["attrs"]["Toolbar"]["logo"] is None


@pytest.mark.unit
def test_theme_uses_poppins_font() -> None:
    """The theme's type treatment must pick up Poppins for axes/titles/legend."""
    theme_json = build_theme_json()
    assert theme_json["attrs"]["Axis"]["axis_label_text_font"] == "Poppins"
    assert theme_json["attrs"]["Title"]["text_font"] == "Poppins"
    assert theme_json["attrs"]["Legend"]["label_text_font"] == "Poppins"
