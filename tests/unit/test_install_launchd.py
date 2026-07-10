"""Unit tests for the plist builder in scripts/install_launchd.py.

Imported by file path — the script lives outside the package.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_SCRIPT = Path(__file__).parents[2] / "scripts" / "install_launchd.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("install_launchd", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


install_launchd = _load_module()


class TestAgentPath:
    def test_path_includes_node_dir_for_cockpit_deploy(self, monkeypatch):
        """The agent PATH must resolve npx — cockpit deploy shells out to it."""
        monkeypatch.setattr(
            install_launchd.shutil,
            "which",
            lambda name: {
                "uv": "/Users/u/.local/bin/uv",
                "npx": "/opt/homebrew/bin/npx",
            }.get(name),
        )

        plist = install_launchd.build_plist("daily", {"Hour": 6, "Minute": 0})
        path_env = plist["EnvironmentVariables"]["PATH"]

        assert "/opt/homebrew/bin" in path_env.split(":")
        assert "/Users/u/.local/bin" in path_env.split(":")

    def test_path_falls_back_to_homebrew_bin_when_npx_missing(
        self, monkeypatch, capsys
    ):
        monkeypatch.setattr(install_launchd.shutil, "which", lambda name: None)

        plist = install_launchd.build_plist("daily", {"Hour": 6, "Minute": 0})
        path_env = plist["EnvironmentVariables"]["PATH"]

        assert "/opt/homebrew/bin" in path_env.split(":")
        # The fallback must not be silent — it's only right for Homebrew node.
        assert "WARNING: npx not found" in capsys.readouterr().err

    def test_path_order_is_uv_then_node_then_system(self, monkeypatch):
        """Deliberate precedence: uv first, node next, system dirs last."""
        monkeypatch.setattr(
            install_launchd.shutil,
            "which",
            lambda name: {
                "uv": "/Users/u/.local/bin/uv",
                "npx": "/opt/homebrew/bin/npx",
            }.get(name),
        )

        plist = install_launchd.build_plist("daily", {"Hour": 6, "Minute": 0})
        entries = plist["EnvironmentVariables"]["PATH"].split(":")

        assert entries == [
            "/Users/u/.local/bin",
            "/opt/homebrew/bin",
            "/usr/local/bin",
            "/usr/bin",
            "/bin",
            "/usr/sbin",
            "/sbin",
        ]

    def test_path_entries_are_deduplicated(self, monkeypatch):
        """uv and npx in the same dir must not produce duplicate PATH entries."""
        monkeypatch.setattr(
            install_launchd.shutil,
            "which",
            lambda name: {
                "uv": "/usr/local/bin/uv",
                "npx": "/usr/local/bin/npx",
            }.get(name),
        )

        plist = install_launchd.build_plist("daily", {"Hour": 6, "Minute": 0})
        entries = plist["EnvironmentVariables"]["PATH"].split(":")

        assert len(entries) == len(set(entries))
        assert entries.count("/usr/local/bin") == 1
