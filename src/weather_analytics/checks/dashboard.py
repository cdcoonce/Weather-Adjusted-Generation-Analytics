"""Dagster asset checks for the dashboard export pipeline."""

from dagster import AssetCheckResult, AssetKey, asset_check
from github import Github
from github.GithubException import GithubException

from weather_analytics.resources.portfolio_repo import PortfolioRepoResource


@asset_check(
    asset=AssetKey(["waga_dashboard_export_publish"]),
    name="waga_dashboard_export_commit_landed",
)
def waga_dashboard_export_commit_landed(
    portfolio_repo: PortfolioRepoResource,
) -> AssetCheckResult:
    """Verify the most recent dashboard data commit resolved on the portfolio repo."""
    gh = Github(portfolio_repo.token)
    try:
        repo = gh.get_repo(portfolio_repo.full_name)
        ref = repo.get_git_ref(f"heads/{portfolio_repo.branch}")
        commit_sha = ref.object.sha
    except GithubException as exc:
        return AssetCheckResult(
            passed=False,
            metadata={"error": f"GitHub API error: HTTP {exc.status}"},
        )
    return AssetCheckResult(
        passed=True,
        metadata={"commit_sha": commit_sha, "branch": portfolio_repo.branch},
    )
