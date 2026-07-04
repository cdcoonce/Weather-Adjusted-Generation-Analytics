"""Deploy dist/ to Cloudflare Pages via `npx wrangler pages deploy`.

Copied from afk-cockpit. `npx` (not a bare `wrangler`) because launchd's minimal
PATH won't resolve a global install. `wrangler` reads CLOUDFLARE_API_TOKEN and
CLOUDFLARE_ACCOUNT_ID from the environment.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path

Runner = Callable[[Sequence[str]], str]

DEFAULT_PROJECT_NAME = "waga-dashboard"


def _default_runner(argv: Sequence[str]) -> str:
    return subprocess.run(list(argv), capture_output=True, text=True, check=True).stdout


def deploy(
    dist_dir: Path,
    project_name: str = DEFAULT_PROJECT_NAME,
    branch: str = "main",
    runner: Runner = _default_runner,
) -> str:
    """Upload dist_dir to Cloudflare Pages as deployment. Returns wrangler stdout."""
    return runner(
        [
            "npx",
            "--yes",
            "wrangler",
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
