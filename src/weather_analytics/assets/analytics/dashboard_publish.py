"""Dashboard publish asset: render + deploy the static cockpit dashboard.

``waga_dashboard_publish`` does what ``python -m weather_analytics.cockpit
build`` then ``... deploy`` do on the launchd host (see
``scripts/run_scheduled.py`` ``POST_STEPS``), but as a Dagster asset that
calls the same underlying functions the CLI calls directly — not a
subprocess of the CLI. This lets the daily job on the home server publish
the dashboard as part of the run instead of a separate launchd step.
"""

import re
import subprocess
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from weather_analytics.cockpit import config
from weather_analytics.cockpit.cloudflare import DEFAULT_PROJECT_NAME, deploy
from weather_analytics.cockpit.data import load_dataset
from weather_analytics.cockpit.render import render_dashboard

# Matches the deployment URL wrangler prints on success, e.g.
# "✨ Deployment complete! Take a peek over at https://<hash>.waga-dashboard.pages.dev"
_DEPLOYMENT_URL_RE = re.compile(r"https://\S+\.pages\.dev\S*")


def _extract_deployment_url(stdout: str) -> str | None:
    """Best-effort parse of the Pages URL from wrangler's stdout.

    Returns ``None`` when the expected pattern isn't found — wrangler's
    output format isn't a stable contract, so this is metadata-on-a-best-
    effort basis, never something the asset depends on to succeed.
    """
    match = _DEPLOYMENT_URL_RE.search(stdout)
    return match.group(0) if match else None


@asset(
    name="waga_dashboard_publish",
    group_name="dashboard",
    deps=["waga_dashboard_export_build"],
)
def waga_dashboard_publish(context: AssetExecutionContext) -> MaterializeResult:
    """Render the static dashboard from the fresh JSON exports and deploy it.

    Calls ``cockpit.data.load_dataset`` + ``cockpit.render.render_dashboard``
    (the ``cockpit build`` step) and then ``cockpit.cloudflare.deploy`` (the
    ``cockpit deploy`` step) directly, in-process — no subprocess of the
    ``weather_analytics.cockpit`` CLI itself. ``deploy`` shells out to
    wrangler; that subprocess call is the only one this asset makes.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context.

    Returns
    -------
    MaterializeResult
        Metadata about the render (asset/row counts) and, when parseable
        from wrangler's stdout, the deployment URL.

    Raises
    ------
    dagster.Failure
        If ``wrangler pages deploy`` exits non-zero. The failure carries
        wrangler's stderr as metadata; never the environment.
    """
    export_dir = Path(config.DEFAULT_EXPORT_DIR)
    dist_dir = Path(config.DEFAULT_DIST_DIR)
    out_path = Path(config.DEFAULT_OUT)

    dataset = load_dataset(export_dir)
    render_dashboard(dataset, out_path)
    context.log.info(
        "Rendered dashboard to %s (%d assets, %d daily rows)",
        out_path,
        dataset.manifest.asset_count,
        len(dataset.daily),
    )

    try:
        stdout = deploy(dist_dir, project_name=DEFAULT_PROJECT_NAME, branch="main")
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise Failure(
            description=(
                "wrangler pages deploy failed "
                f"(exit {exc.returncode}): {stderr or 'no stderr captured'}"
            ),
            metadata={"wrangler_stderr": MetadataValue.text(stderr)},
        ) from exc

    context.log.info(stdout)

    metadata: dict[str, object] = {
        "asset_count": dataset.manifest.asset_count,
        "daily_rows": len(dataset.daily),
        "weather_rows": len(dataset.weather),
        "dist_dir": MetadataValue.path(str(dist_dir)),
    }
    deployment_url = _extract_deployment_url(stdout)
    if deployment_url:
        metadata["deployment_url"] = MetadataValue.url(deployment_url)

    return MaterializeResult(metadata=metadata)
