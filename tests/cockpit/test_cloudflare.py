from pathlib import Path

from weather_analytics.cockpit import cloudflare


def _recording_runner(calls):
    def run(argv):
        calls.append(list(argv))
        return "ok"
    return run


def test_deploy_invokes_npx_wrangler_with_project_and_branch():
    calls = []
    out = cloudflare.deploy(Path("/repo/dist"), runner=_recording_runner(calls))
    assert out == "ok"
    assert calls == [[
        "npx", "--yes", "wrangler", "pages", "deploy", "/repo/dist",
        "--project-name", "waga-dashboard", "--branch", "main", "--commit-dirty=true",
    ]]


def test_deploy_respects_overrides():
    calls = []
    cloudflare.deploy(Path("/d"), project_name="other", branch="dev", runner=_recording_runner(calls))
    assert "--project-name" in calls[0]
    assert calls[0][calls[0].index("--project-name") + 1] == "other"
    assert calls[0][calls[0].index("--branch") + 1] == "dev"
