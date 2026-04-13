"""Portfolio repo resource for cross-repo GitHub Contents API pushes.

Used by the ``waga_dashboard_export_publish`` asset to push dashboard
data files from Dagster Cloud to the static portfolio repo that hosts
the Panel app via GitHub Pages.

PyGithub debug logging is intentionally not enabled anywhere — enabling
it would leak the PAT in request headers. Do not add debug logging.
"""

from dagster import ConfigurableResource


class PortfolioRepoResource(ConfigurableResource):
    """Dagster resource wrapping a GitHub fine-grained PAT for the portfolio repo.

    The PAT must be scoped to ``contents:write`` on the portfolio repo
    only. Note that fine-grained PATs cannot path-restrict within a repo,
    so a leaked PAT could touch any file in the portfolio repo — mitigate
    via quarterly rotation and commit history review.

    Parameters
    ----------
    owner : str
        Portfolio repo owner (GitHub username or org).
    name : str
        Portfolio repo name.
    branch : str
        Target branch in the portfolio repo (default ``main``).
    token : str
        Fine-grained PAT with ``contents:write`` scope.
    """

    owner: str
    name: str
    branch: str
    token: str

    @property
    def full_name(self) -> str:
        """Return ``owner/name`` repo slug."""
        return f"{self.owner}/{self.name}"
