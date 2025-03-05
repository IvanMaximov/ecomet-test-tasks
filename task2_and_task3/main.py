import asyncio
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Final, Any

import aiohttp
from aiolimiter import AsyncLimiter

from config import (
    GITHUB_ACCESS_TOKEN,
    MAX_CONCURRENT_REQUESTS,
    REQUESTS_PER_SECOND,
    CLICKHOUSE_DSN,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    REPOSITORY_LIMIT
)
from db_actions import ClickHouseManager
from logging_presets import logger

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


@dataclass
class RepositoryAuthorCommitsNum:
    """Represents commit count per author."""
    author: str
    commits_num: int


@dataclass
class Repository:
    """Represents a GitHub repository with metadata and commit details."""
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class GithubReposScraper:
    """Handles retrieving and processing GitHub repositories asynchronously."""

    def __init__(self, access_token: str, max_concurrent_requests: int = 10, requests_per_second: int = 5):
        self._session = aiohttp.ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._limiter = AsyncLimiter(requests_per_second, 1)

    async def _make_request(self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None) -> Any:
        """Sends an HTTP request to the GitHub API and returns the response data."""
        url = f"{GITHUB_API_BASE_URL}/{endpoint}"
        logger.info(f"Requesting: {method} {url} with params {params}")

        try:
            async with self._limiter, self._semaphore, self._session.request(method, url, params=params) as response:
                response.raise_for_status()
                logger.info(f"Successful response: {response.status} {method} {url}")
                return await response.json()

        except aiohttp.ClientError as e:
            logger.error(f"Request failed: {method} {url} - {e}", exc_info=True)
            raise

    async def _get_top_repositories(self, limit: int) -> list[dict[str, Any]]:
        """Fetches the top repositories sorted by star count."""
        logger.info(f"Fetching top {limit} repositories by stars...")

        data = await self._make_request(
            endpoint="search/repositories",
            params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": limit},
        )

        repositories = data.get("items", [])
        logger.info(f"Retrieved {len(repositories)} repositories.")
        return repositories

    async def _get_repository_commits(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """Fetches commits for a given repository from the last 24 hours."""
        since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        logger.info(f"Fetching commits for {owner}/{repo} since {since}...")

        commits = await self._make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params={"since": since, "per_page": 100},
        )

        logger.info(f"Retrieved {len(commits)} commits for {owner}/{repo}.")
        return commits

    async def _process_repository(self, repo_data: dict[str, Any], position: int) -> Repository:
        """Processes repository data and collects commit information."""
        name = repo_data.get("name", "UNKNOWN")
        logger.info(f"Processing repository {position}: {name}.")

        try:
            owner = repo_data["owner"]["login"]
            commits = await self._get_repository_commits(owner, name)

            authors_count = {}
            for commit in commits:
                if commit.get("author"):
                    author = commit["author"].get("login", "unknown")
                    authors_count[author] = authors_count.get(author, 0) + 1

            authors_commits = [RepositoryAuthorCommitsNum(author, count) for author, count in authors_count.items()]

            repo = Repository(
                name=name,
                owner=owner,
                position=position,
                stars=repo_data["stargazers_count"],
                watchers=repo_data["watchers_count"],
                forks=repo_data["forks_count"],
                language=repo_data.get("language", "Unknown"),
                authors_commits_num_today=authors_commits
            )

            logger.info(f"Successfully processed repository {position}: {name}.")
            return repo

        except KeyError as e:
            logger.error(f"Data structure error in repository {position}: missing key {e}", exc_info=True)
            raise

    async def get_repositories(self, limit: int = 100) -> list[Repository]:
        """Retrieves a list of repositories along with their commit authors."""
        top_repos = await self._get_top_repositories(limit)
        tasks = [self._process_repository(repo, idx + 1) for idx, repo in enumerate(top_repos)]
        return await asyncio.gather(*tasks)

    async def close(self):
        """Closes the HTTP session."""
        await self._session.close()


async def main():
    """Main function to scrape GitHub repositories and store data in ClickHouse."""
    github_scraper = GithubReposScraper(GITHUB_ACCESS_TOKEN, MAX_CONCURRENT_REQUESTS, REQUESTS_PER_SECOND)
    clickhouse_manager = ClickHouseManager(CLICKHOUSE_DSN, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    try:
        repos_data = await github_scraper.get_repositories(REPOSITORY_LIMIT)
        date_today = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        repo_data = [(r.name, r.owner, r.stars, r.watchers, r.forks, r.language, date_today) for r in repos_data]
        positions_data = [(date_today, r.name, r.position) for r in repos_data]
        commits_data = [(date_today, r.name, ac.author, ac.commits_num) for r in repos_data for ac in r.authors_commits_num_today]

        await asyncio.gather(
            clickhouse_manager.save_repositories(repo_data),
            clickhouse_manager.save_positions(positions_data),
            clickhouse_manager.save_commits(commits_data)
        )

    finally:
        await github_scraper.close()
        await clickhouse_manager.close()


if __name__ == '__main__':
    asyncio.run(main())
