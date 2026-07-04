from pathlib import Path

import pytest

from weather_analytics.cockpit.cli import main

FIXTURES = Path(__file__).parent / "fixtures"


def test_build_writes_index(tmp_path):
    out = tmp_path / "dist" / "index.html"
    code = main(["build", "--export-dir", str(FIXTURES), "--out", str(out)])
    assert code == 0
    assert out.exists()
    html = out.read_text(encoding="utf-8")
    assert "weather-adjusted" in html.lower()
    assert "cockpit-data" in html


def test_deploy_calls_cloudflare(monkeypatch, tmp_path):
    seen = {}

    def fake_deploy(
        dist_dir, project_name="waga-dashboard", branch="main", runner=None
    ):
        seen["dist"] = str(dist_dir)
        seen["project"] = project_name
        return "deployed"

    monkeypatch.setattr("weather_analytics.cockpit.cli.deploy", fake_deploy)
    code = main(["deploy", "--dist", str(tmp_path)])
    assert code == 0
    assert seen["dist"] == str(tmp_path)
    assert seen["project"] == "waga-dashboard"


def test_unknown_command_errors():
    with pytest.raises(SystemExit):
        main(["frobnicate"])
