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


def test_render_is_valid_parseable_html(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)

    class _P(HTMLParser):
        pass

    _P().feed(out.read_text(encoding="utf-8"))  # raises on malformed markup
