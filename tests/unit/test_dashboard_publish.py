"""Unit tests for ``weather_analytics.assets.analytics.dashboard_publish``.

All Cloudflare/wrangler interaction is faked — no real ``wrangler`` process
runs. Exercises the render -> deploy call chain, failure translation
(``subprocess.CalledProcessError`` -> ``dagster.Failure``), and the
best-effort deployment-URL metadata parse.
"""

from __future__ import annotations

import inspect
import subprocess
from dataclasses import dataclass

import pytest
from dagster import Failure, MaterializeResult, build_asset_context

from weather_analytics.assets.analytics import dashboard_publish as _publish_module
from weather_analytics.assets.analytics.dashboard_publish import (
    _extract_deployment_url,
    waga_dashboard_publish,
)

pytestmark = pytest.mark.unit


@dataclass
class _FakeManifest:
    asset_count: int = 42


@dataclass
class _FakeDataset:
    manifest: _FakeManifest
    daily: list
    weather: list


def _fake_dataset() -> _FakeDataset:
    return _FakeDataset(manifest=_FakeManifest(), daily=[1, 2, 3], weather=[1, 2])


class TestExtractDeploymentUrl:
    def test_finds_pages_dev_url(self):
        stdout = (
            "Uploaded 3 files\n"
            "✨ Deployment complete! Take a peek over at "
            "https://841e3f01.waga-dashboard.pages.dev\n"
        )
        assert (
            _extract_deployment_url(stdout)
            == "https://841e3f01.waga-dashboard.pages.dev"
        )

    def test_returns_none_when_not_present(self):
        assert _extract_deployment_url("no url here") is None


class TestWagaDashboardPublish:
    def test_renders_then_deploys_and_reports_url(self, monkeypatch):
        calls = {}

        def fake_load_dataset(export_dir):
            calls["export_dir"] = export_dir
            return _fake_dataset()

        def fake_render_dashboard(dataset, out_path):
            calls["render"] = (dataset, out_path)

        def fake_deploy(dist_dir, *, project_name, branch):
            calls["deploy"] = (dist_dir, project_name, branch)
            return "Deployment complete! https://abc123.waga-dashboard.pages.dev\n"

        monkeypatch.setattr(_publish_module, "load_dataset", fake_load_dataset)
        monkeypatch.setattr(_publish_module, "render_dashboard", fake_render_dashboard)
        monkeypatch.setattr(_publish_module, "deploy", fake_deploy)

        context = build_asset_context()
        result = waga_dashboard_publish(context)

        assert isinstance(result, MaterializeResult)
        assert "render" in calls
        assert calls["deploy"][1] == "waga-dashboard"
        assert calls["deploy"][2] == "main"
        metadata = result.metadata
        assert metadata["asset_count"] == 42
        assert metadata["daily_rows"] == 3
        assert metadata["weather_rows"] == 2
        assert (
            metadata["deployment_url"].url
            == "https://abc123.waga-dashboard.pages.dev"
        )

    def test_missing_url_omits_metadata_key(self, monkeypatch):
        monkeypatch.setattr(
            _publish_module, "load_dataset", lambda export_dir: _fake_dataset()
        )
        monkeypatch.setattr(
            _publish_module, "render_dashboard", lambda dataset, out_path: None
        )
        monkeypatch.setattr(
            _publish_module,
            "deploy",
            lambda dist_dir, project_name, branch: "no url in this output",
        )

        result = waga_dashboard_publish(build_asset_context())

        assert "deployment_url" not in result.metadata

    def test_wrangler_failure_raises_dagster_failure_with_stderr(self, monkeypatch):
        monkeypatch.setattr(
            _publish_module, "load_dataset", lambda export_dir: _fake_dataset()
        )
        monkeypatch.setattr(
            _publish_module, "render_dashboard", lambda dataset, out_path: None
        )

        def fake_deploy(dist_dir, project_name, branch):
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=["npx", "wrangler", "pages", "deploy"],
                output="",
                stderr="Error: Authentication error",
            )

        monkeypatch.setattr(_publish_module, "deploy", fake_deploy)

        with pytest.raises(Failure) as exc_info:
            waga_dashboard_publish(build_asset_context())

        assert "Authentication error" in str(exc_info.value.description)
        assert "Authentication error" in str(
            exc_info.value.metadata["wrangler_stderr"]
        )

    def test_never_logs_environment_variables(self, monkeypatch):
        """Regression guard: the asset must not read/log os.environ anywhere."""
        source = inspect.getsource(_publish_module)
        assert "os.environ" not in source
        assert "getenv" not in source
