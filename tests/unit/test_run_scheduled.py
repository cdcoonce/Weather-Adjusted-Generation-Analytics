"""Unit tests for the hardening helpers in scripts/run_scheduled.py.

The script is stdlib-only and lives outside the package (launchd invokes it
with the system python3), so it is imported here by file path.
"""

from __future__ import annotations

import ast
import importlib.util
import inspect
import io
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.unit

_SCRIPT = Path(__file__).parents[2] / "scripts" / "run_scheduled.py"
_NO_DNS = "no dns"
_UNAVAILABLE = "unavailable"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_scheduled", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_scheduled = _load_module()


class FakeClock:
    """Deterministic stand-in for time.monotonic + time.sleep."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class FlushCountingLog(io.StringIO):
    """StringIO that counts flush() calls."""

    def __init__(self) -> None:
        super().__init__()
        self.flushes = 0

    def flush(self) -> None:
        self.flushes += 1
        super().flush()


class TestWaitForNetwork:
    def test_returns_true_immediately_when_dns_resolves(self):
        clock = FakeClock()
        calls = []

        def probe(host, port):
            calls.append((host, port))
            return []

        ok = run_scheduled.wait_for_network(
            probe=probe,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
            emit=lambda _msg: None,
        )

        assert ok is True
        assert len(calls) == 1
        assert clock.sleeps == []

    def test_retries_until_dns_comes_up(self):
        clock = FakeClock()
        outcomes = iter([socket.gaierror(_NO_DNS), socket.gaierror(_NO_DNS), None])
        messages: list[str] = []

        def probe(host, port):
            outcome = next(outcomes)
            if outcome is not None:
                raise outcome
            return []

        ok = run_scheduled.wait_for_network(
            timeout_s=300.0,
            interval_s=15.0,
            probe=probe,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
            emit=messages.append,
        )

        assert ok is True
        assert clock.sleeps == [15.0, 15.0]
        assert any("retrying" in message for message in messages)

    def test_gives_up_after_timeout_and_returns_false(self):
        clock = FakeClock()
        messages: list[str] = []

        def probe(host, port):
            raise socket.gaierror(_NO_DNS)

        # timeout deliberately NOT a multiple of interval, so the final sleep
        # must be clamped to the remaining budget (5 s) — pins the
        # min(interval_s, remaining) deadline arithmetic.
        ok = run_scheduled.wait_for_network(
            timeout_s=50.0,
            interval_s=15.0,
            probe=probe,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
            emit=messages.append,
        )

        assert ok is False
        assert clock.sleeps == [15.0, 15.0, 15.0, 5.0]
        assert sum(clock.sleeps) == pytest.approx(50.0)
        assert any("proceeding anyway" in message for message in messages)

    def test_production_defaults_are_pinned(self):
        """main() calls wait_for_network(emit=...) relying on these defaults."""
        defaults = {
            name: parameter.default
            for name, parameter in inspect.signature(
                run_scheduled.wait_for_network
            ).parameters.items()
        }
        assert defaults["host"] == "pypi.org"
        assert defaults["port"] == 443
        assert defaults["timeout_s"] == 300.0
        assert defaults["interval_s"] == 15.0


class TestRunStepWithRetries:
    def _runner(self, returncodes: list[int]):
        """Fake subprocess.run returning scripted exit codes in order."""
        remaining = iter(returncodes)
        calls: list[list[str]] = []

        def run(cmd, **kwargs):
            calls.append(list(cmd))
            return SimpleNamespace(returncode=next(remaining))

        return run, calls

    def test_success_first_try_runs_once_and_never_sleeps(self):
        clock = FakeClock()
        run, calls = self._runner([0])

        code = run_scheduled.run_step_with_retries(
            ["uv", "sync"],
            label="pre-step",
            emit=lambda _msg: None,
            log_file=io.StringIO(),
            runner=run,
            sleep=clock.sleep,
        )

        assert code == 0
        assert len(calls) == 1
        assert clock.sleeps == []

    def test_retries_then_succeeds(self):
        clock = FakeClock()
        run, calls = self._runner([1, 0])
        messages: list[str] = []

        code = run_scheduled.run_step_with_retries(
            ["uv", "sync"],
            label="pre-step",
            emit=messages.append,
            log_file=io.StringIO(),
            attempts=3,
            retry_delay_s=30.0,
            runner=run,
            sleep=clock.sleep,
        )

        assert code == 0
        assert len(calls) == 2
        assert clock.sleeps == [30.0]
        assert any("retrying" in message for message in messages)

    def test_exhausted_attempts_returns_last_exit_code(self):
        clock = FakeClock()
        run, calls = self._runner([1, 7])

        code = run_scheduled.run_step_with_retries(
            ["dagster", "asset", "materialize"],
            label="step 1/3",
            emit=lambda _msg: None,
            log_file=io.StringIO(),
            attempts=2,
            retry_delay_s=60.0,
            runner=run,
            sleep=clock.sleep,
        )

        assert code == 7
        assert len(calls) == 2
        assert clock.sleeps == [60.0]

    def test_default_attempts_is_two(self):
        clock = FakeClock()
        run, calls = self._runner([1, 1])

        code = run_scheduled.run_step_with_retries(
            ["anything"],
            label="step",
            emit=lambda _msg: None,
            log_file=io.StringIO(),
            runner=run,
            sleep=clock.sleep,
        )

        assert code == 1
        assert len(calls) == 2

    def test_subprocess_wiring_and_log_flush(self):
        """Steps must run from the repo root with output into the run log."""
        log = FlushCountingLog()
        seen_kwargs: list[dict] = []

        def run(cmd, **kwargs):
            seen_kwargs.append(kwargs)
            return SimpleNamespace(returncode=0)

        code = run_scheduled.run_step_with_retries(
            ["anything"],
            label="step",
            emit=lambda _msg: None,
            log_file=log,
            runner=run,
            sleep=lambda _s: None,
        )

        assert code == 0
        assert seen_kwargs[0]["cwd"] == run_scheduled.REPO
        assert seen_kwargs[0]["stdout"] is log
        assert seen_kwargs[0]["stderr"] == subprocess.STDOUT
        assert log.flushes >= 1


class TestHoldWakeAssertion:
    def test_spawns_caffeinate_sidecar_bound_to_pid(self):
        spawned: list[list[str]] = []

        def popen(cmd, **kwargs):
            spawned.append(list(cmd))
            return SimpleNamespace(pid=999)

        process = run_scheduled.hold_wake_assertion(
            1234, emit=lambda _msg: None, popen=popen
        )

        assert process is not None
        assert spawned == [["/usr/bin/caffeinate", "-i", "-s", "-w", "1234"]]

    def test_missing_caffeinate_is_non_fatal(self):
        messages: list[str] = []

        def popen(cmd, **kwargs):
            raise FileNotFoundError(_UNAVAILABLE)

        process = run_scheduled.hold_wake_assertion(
            1234, emit=messages.append, popen=popen
        )

        assert process is None
        assert any("without wake assertion" in message for message in messages)


class TestReportOutcome:
    def _report(self, returncode, monkeypatch, url=None):
        notifications: list[list[str]] = []
        pings: list[str] = []
        if url is None:
            monkeypatch.delenv("WAGA_HEALTHCHECK_URL", raising=False)
        else:
            monkeypatch.setenv("WAGA_HEALTHCHECK_URL", url)

        def runner(cmd, **kwargs):
            notifications.append(list(cmd))
            return SimpleNamespace(returncode=0)

        def urlopen(target, timeout=None):
            pings.append(target)
            return SimpleNamespace(status=200)

        run_scheduled.report_outcome(
            "daily",
            returncode,
            Path("/x/logs/scheduled-daily-1.log"),
            emit=lambda _msg: None,
            runner=runner,
            urlopen=urlopen,
        )
        return notifications, pings

    def test_success_pings_but_does_not_notify(self, monkeypatch):
        notifications, pings = self._report(0, monkeypatch, url="https://hc.io/abc")

        assert notifications == []
        assert pings == ["https://hc.io/abc"]

    def test_failure_notifies_and_pings_fail_endpoint(self, monkeypatch):
        notifications, pings = self._report(7, monkeypatch, url="https://hc.io/abc")

        assert len(notifications) == 1
        assert notifications[0][0] == "/usr/bin/osascript"
        assert "daily" in notifications[0][2]
        assert pings == ["https://hc.io/abc/fail"]

    def test_no_url_means_no_ping(self, monkeypatch):
        _notifications, pings = self._report(0, monkeypatch, url=None)

        assert pings == []

    def test_ping_failure_is_swallowed(self, monkeypatch):
        monkeypatch.setenv("WAGA_HEALTHCHECK_URL", "https://hc.io/abc")
        messages: list[str] = []

        def urlopen(target, timeout=None):
            raise OSError(_UNAVAILABLE)

        run_scheduled.report_outcome(
            "daily",
            0,
            Path("/x/logs/scheduled-daily-1.log"),
            emit=messages.append,
            runner=lambda cmd, **kw: SimpleNamespace(returncode=0),
            urlopen=urlopen,
        )

        assert any("healthcheck ping failed" in message for message in messages)


class TestMainWiring:
    """Pin main()'s chain: preflight -> uv sync --inexact -> steps -> post-steps."""

    def _run_main(self, monkeypatch, tmp_path, fail_on=None):
        calls: list[dict] = []

        def fake_wait(**kwargs):
            calls.append({"kind": "network-wait"})
            return True

        def fake_step(cmd, *, label, attempts=2, **_kwargs):
            calls.append(
                {"kind": "step", "cmd": list(cmd), "label": label, "attempts": attempts}
            )
            return 5 if fail_on is not None and fail_on in label else 0

        def fake_wake(pid, **_kwargs):
            calls.append({"kind": "wake-assertion", "pid": pid})

        def fake_outcome(job, returncode, log_path, **_kwargs):
            calls.append({"kind": "outcome", "job": job, "returncode": returncode})

        monkeypatch.setattr(run_scheduled, "REPO", tmp_path)
        monkeypatch.setattr(run_scheduled, "wait_for_network", fake_wait)
        monkeypatch.setattr(run_scheduled, "run_step_with_retries", fake_step)
        monkeypatch.setattr(run_scheduled, "hold_wake_assertion", fake_wake)
        monkeypatch.setattr(run_scheduled, "report_outcome", fake_outcome)
        monkeypatch.setenv("DAGSTER_HOME", "sentinel")
        monkeypatch.setattr(sys, "argv", ["run_scheduled.py", "daily"])
        return run_scheduled.main(), calls

    def test_daily_chain_order_and_retry_budgets(self, monkeypatch, tmp_path):
        code, calls = self._run_main(monkeypatch, tmp_path)

        assert code == 0
        # Wake assertion is taken before anything else (the machine may
        # re-sleep ~3 min into a DarkWake window otherwise).
        assert calls[0]["kind"] == "wake-assertion"
        assert calls[1]["kind"] == "network-wait"
        # The outcome is always reported, with the chain's exit code.
        assert calls[-1] == {"kind": "outcome", "job": "daily", "returncode": 0}
        # Pre-step: uv sync --inexact (inexact so dev extras survive), 3 attempts.
        step_calls = [c for c in calls if c["kind"] == "step"]
        sync_call = step_calls[0]
        assert sync_call["cmd"][1:] == ["sync", "--inexact"]
        assert sync_call["attempts"] == 3
        # 3 dagster steps + 2 cockpit post-steps, default retry budget.
        dagster_steps = step_calls[1:4]
        assert all(
            c["cmd"][1:5] == ["run", "python", "-m", "dagster"] for c in dagster_steps
        )
        assert all(c["attempts"] == 2 for c in dagster_steps)
        post_steps = step_calls[4:]
        assert len(post_steps) == 2
        assert all("cockpit" in " ".join(c["cmd"]) for c in post_steps)

    def test_pre_step_failure_aborts_before_any_dagster_step(
        self, monkeypatch, tmp_path
    ):
        code, calls = self._run_main(monkeypatch, tmp_path, fail_on="uv sync")

        assert code == 5
        steps = [c for c in calls if c["kind"] == "step"]
        assert len(steps) == 1  # only the pre-step ran
        # The failure is still reported (notification + healthcheck /fail).
        assert calls[-1] == {"kind": "outcome", "job": "daily", "returncode": 5}


class TestLaunchdInterpreterContract:
    """launchd runs these scripts with the system python3 (3.9), stdlib only."""

    _SCRIPTS = (
        Path(__file__).parents[2] / "scripts" / "run_scheduled.py",
        Path(__file__).parents[2] / "scripts" / "install_launchd.py",
    )

    @pytest.mark.parametrize("script", _SCRIPTS, ids=lambda p: p.name)
    def test_parses_as_python_39(self, script):
        ast.parse(script.read_text(), filename=str(script), feature_version=(3, 9))

    @pytest.mark.parametrize("script", _SCRIPTS, ids=lambda p: p.name)
    def test_imports_are_stdlib_only(self, script):
        tree = ast.parse(script.read_text())
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported.add(node.module.split(".")[0])
        non_stdlib = imported - set(sys.stdlib_module_names) - {"__future__"}
        assert not non_stdlib, f"non-stdlib imports break under launchd: {non_stdlib}"
