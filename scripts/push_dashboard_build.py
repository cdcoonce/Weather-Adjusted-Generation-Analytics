"""Push compiled dashboard build output to the portfolio repo.

Used by ``.github/workflows/build-dashboard.yml`` as the final deploy step.
Reads files from ``dashboard_build/`` in the current working directory and
pushes them to the portfolio repo's ``<target_dir>/`` directory via the
GitHub Contents API.

Environment variables (all required):
- ``WAGA_PORTFOLIO_REPO_TOKEN`` — fine-grained PAT, ``contents:write``.
- ``PORTFOLIO_OWNER`` — repo owner, e.g. ``cdcoonce``.
- ``PORTFOLIO_NAME`` — repo name, e.g. ``charleslikesdata``.
- ``PORTFOLIO_BRANCH`` — target branch.
- ``PORTFOLIO_TARGET_DIR`` — target subdirectory, e.g. ``dashboard``.

PyGithub debug logging is intentionally never enabled — it would leak the
PAT in request headers.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from github import Github
from github.GithubException import GithubException

BUILD_DIR = Path("dashboard_build")
_HTTP_NOT_FOUND = 404


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Missing required env var: {name}", file=sys.stderr)
        sys.exit(1)
    return value


def main() -> None:
    """Entry point — upload every file under ``BUILD_DIR`` to the target dir."""
    token = _require_env("WAGA_PORTFOLIO_REPO_TOKEN")
    owner = _require_env("PORTFOLIO_OWNER")
    name = _require_env("PORTFOLIO_NAME")
    branch = _require_env("PORTFOLIO_BRANCH")
    target_dir = _require_env("PORTFOLIO_TARGET_DIR").strip("/")

    if not BUILD_DIR.exists():
        print(f"Build dir missing: {BUILD_DIR}", file=sys.stderr)
        sys.exit(1)

    files = sorted(
        p for p in BUILD_DIR.rglob("*") if p.is_file() and "data" not in p.parts
    )
    if not files:
        print("No build artifacts to push.", file=sys.stderr)
        sys.exit(1)

    gh = Github(token)
    repo = gh.get_repo(f"{owner}/{name}")
    build_sha = os.environ.get("GITHUB_SHA", "local")[:8]
    commit_message_template = f"chore(dashboard): rebuild app from {build_sha}"

    for local_path in files:
        rel = local_path.relative_to(BUILD_DIR)
        remote_path = f"{target_dir}/{rel.as_posix()}"
        content = local_path.read_bytes()
        try:
            existing = repo.get_contents(remote_path, ref=branch)
            if isinstance(existing, list):
                print(
                    f"{remote_path} unexpectedly resolves to a directory; skipping",
                    file=sys.stderr,
                )
                continue
            repo.update_file(
                path=remote_path,
                message=commit_message_template,
                content=content,
                sha=existing.sha,
                branch=branch,
            )
            print(f"Updated {remote_path}")
        except GithubException as exc:
            if exc.status == _HTTP_NOT_FOUND:
                repo.create_file(
                    path=remote_path,
                    message=commit_message_template,
                    content=content,
                    branch=branch,
                )
                print(f"Created {remote_path}")
            else:
                print(
                    f"Failed to push {remote_path}: HTTP {exc.status}",
                    file=sys.stderr,
                )
                raise


if __name__ == "__main__":
    main()
