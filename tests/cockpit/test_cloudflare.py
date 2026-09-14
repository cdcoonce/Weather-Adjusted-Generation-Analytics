import inspect
from pathlib import Path

from weather_analytics.cockpit import cloudflare


def _recording_runner(calls):
    def run(argv):
        calls.append(list(argv))
        return "ok"

    return run


def _which_missing(_name):
    """Simulates the launchd host: no global wrangler install on PATH."""
    return


def _which_found(_name):
    """Simulates the Docker image: wrangler installed globally at build time."""
    return "/usr/local/bin/wrangler"


def test_deploy_falls_back_to_npx_wrangler_when_not_on_path():
    calls = []
    out = cloudflare.deploy(
        Path("/repo/dist"), runner=_recording_runner(calls), which=_which_missing
    )
    assert out == "ok"
    assert calls == [
        [
            "npx",
            "--yes",
            "wrangler",
            "pages",
            "deploy",
            "/repo/dist",
            "--project-name",
            "waga-dashboard",
            "--branch",
            "main",
            "--commit-dirty=true",
        ]
    ]


def test_deploy_uses_wrangler_on_path_when_available():
    calls = []
    out = cloudflare.deploy(
        Path("/repo/dist"), runner=_recording_runner(calls), which=_which_found
    )
    assert out == "ok"
    assert calls == [
        [
            "/usr/local/bin/wrangler",
            "pages",
            "deploy",
            "/repo/dist",
            "--project-name",
            "waga-dashboard",
            "--branch",
            "main",
            "--commit-dirty=true",
        ]
    ]


def test_deploy_respects_overrides():
    calls = []
    cloudflare.deploy(
        Path("/d"),
        project_name="other",
        branch="dev",
        runner=_recording_runner(calls),
        which=_which_missing,
    )
    assert "--project-name" in calls[0]
    assert calls[0][calls[0].index("--project-name") + 1] == "other"
    assert calls[0][calls[0].index("--branch") + 1] == "dev"


def test_deploy_default_which_resolves_from_shutil():
    """Without an injected `which`, deploy() falls back to shutil.which — pins
    the default argument so a real PATH lookup happens in production."""
    sig = inspect.signature(cloudflare.deploy)
    assert sig.parameters["which"].default is cloudflare.shutil.which
