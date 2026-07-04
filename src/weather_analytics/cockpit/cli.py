"""`python -m weather_analytics.cockpit build|deploy|serve`."""

from __future__ import annotations

import argparse
from pathlib import Path

from weather_analytics.cockpit import config
from weather_analytics.cockpit.cloudflare import DEFAULT_PROJECT_NAME, deploy
from weather_analytics.cockpit.data import load_dataset
from weather_analytics.cockpit.render import render_dashboard
from weather_analytics.cockpit.serve import serve


def _build(args: argparse.Namespace) -> int:
    """Build the static dashboard."""
    dataset = load_dataset(Path(args.export_dir))
    out = Path(args.out)
    render_dashboard(dataset, out)
    print(f"built {out} from {args.export_dir}")
    return 0


def _deploy(args: argparse.Namespace) -> int:
    """Deploy dist/ to Cloudflare Pages."""
    out = deploy(Path(args.dist), project_name=args.project_name, branch=args.branch)
    print(out.strip() or f"deployed {args.dist} to {config.SITE_URL}")
    return 0


def _serve(args: argparse.Namespace) -> int:
    """Serve dist/ locally."""
    serve(Path(args.dist), port=args.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse CLI args and dispatch to subcommand."""
    parser = argparse.ArgumentParser(prog="weather_analytics.cockpit")
    sub = parser.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="render the static dashboard")
    b.add_argument("--export-dir", default=config.DEFAULT_EXPORT_DIR)
    b.add_argument("--out", default=config.DEFAULT_OUT)
    b.set_defaults(func=_build)

    d = sub.add_parser("deploy", help="deploy dist/ to Cloudflare Pages")
    d.add_argument("--dist", default=config.DEFAULT_DIST_DIR)
    d.add_argument("--project-name", default=DEFAULT_PROJECT_NAME)
    d.add_argument("--branch", default="main")
    d.set_defaults(func=_deploy)

    s = sub.add_parser("serve", help="serve dist/ locally")
    s.add_argument("--dist", default=config.DEFAULT_DIST_DIR)
    s.add_argument("--port", type=int, default=8420)
    s.set_defaults(func=_serve)

    args = parser.parse_args(argv)
    return args.func(args)
