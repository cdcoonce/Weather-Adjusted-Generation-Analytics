from html.parser import HTMLParser

from weather_analytics.cockpit.render import render_dashboard


def test_render_writes_self_contained_html(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)
    html = out.read_text(encoding="utf-8")
    low = html.lower()
    assert "<html" in low
    assert "weather-adjusted" in low  # title/heading present
    # no legacy chart runtimes:
    for banned in ("bokeh", "pyodide", "panel", "plotly"):
        assert banned not in low


def test_render_embeds_json_island(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)
    html = out.read_text(encoding="utf-8")
    assert 'id="cockpit-data"' in html
    assert 'type="application/json"' in html
    assert '"W1"' in html  # dataset serialized into the island


def test_render_inlines_app_js(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out, app_js="/*APPJS_MARKER*/")
    html = out.read_text(encoding="utf-8")
    assert "/*APPJS_MARKER*/" in html


def test_render_defaults_to_bundled_app_js(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)  # no app_js -> bundled file
    html = out.read_text(encoding="utf-8")
    assert "cockpit-data" in html
    assert "addEventListener" in html  # app.js actually inlined


def test_render_is_valid_parseable_html(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)

    class _P(HTMLParser):
        pass

    _P().feed(out.read_text(encoding="utf-8"))  # raises on malformed markup


def test_render_includes_freshness_badge_hook(dataset, tmp_path):
    """The badge is filled client-side; the template must ship the hook."""
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)
    html = out.read_text(encoding="utf-8")
    assert 'id="freshness"' in html
    assert f'data-end="{dataset.manifest.date_range_end}"' in html


def test_bundled_app_js_fills_freshness_badge(dataset, tmp_path):
    """app.js must contain the freshness logic (runs at view time)."""
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)
    html = out.read_text(encoding="utf-8")
    assert 'getElementById("freshness")' in html
    assert "days behind" in html
