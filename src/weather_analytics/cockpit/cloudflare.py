"""Deploy dist/ to Cloudflare Pages via `wrangler pages deploy`.

Copied from afk-cockpit, then adapted for the Docker code-location image
(see the WAGA Dockerfile): the image installs `wrangler` globally at build
time (a pinned version, so a run never needs to hit the npm registry), so
`wrangler` is resolved on PATH via ``shutil.which`` first. On the launchd
host `wrangler` isn't installed globally and launchd's minimal PATH
wouldn't find it anyway, so this falls back to `npx --yes wrangler`, which
resolves an on-demand install. Either way `wrangler` reads
CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID from the environment.
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path

Runner = Callable[[Sequence[str]], str]
Which = Callable[[str], "str | None"]

DEFAULT_PROJECT_NAME = "waga-dashboard"


def _default_runner(argv: Sequence[str]) -> str:
    return subprocess.run(list(argv), capture_output=True, text=True, check=True).stdout


def _wrangler_argv(which: Which = shutil.which) -> list[str]:
    """Resolve the argv prefix that invokes wrangler.

    Prefers a `wrangler` executable on PATH (the Docker image installs one
    at build time); falls back to `npx --yes wrangler` when none is found
    (the launchd host).
    """
    found = which("wrangler")
    if found:
        return [found]
    return ["npx", "--yes", "wrangler"]


def deploy(
    dist_dir: Path,
    project_name: str = DEFAULT_PROJECT_NAME,
    branch: str = "main",
    runner: Runner = _default_runner,
    which: Which = shutil.which,
) -> str:
    """Upload dist_dir to Cloudflare Pages as deployment. Returns wrangler stdout."""
    return runner(
        [
            *_wrangler_argv(which),
            "pages",
            "deploy",
            str(dist_dir),
            "--project-name",
            project_name,
            "--branch",
            branch,
            "--commit-dirty=true",
        ]
    )
