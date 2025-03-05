from aiochclient import ChClient
from aiohttp import ClientSession

from config import BATCH_SIZE
from logging_presets import logger


class ClickHouseManager:
    def __init__(self, clickhouse_dsn: str, clickhouse_user: str, clickhouse_pass: str):
        """Initializes the connection to ClickHouse."""
        self._ch_session = ClientSession()
        self._ch_client = ChClient(self._ch_session, url=clickhouse_dsn, user=clickhouse_user, password=clickhouse_pass)

    async def save_repositories(self, repositories: list[tuple]):
        """Saves repository data to ClickHouse."""
        if not repositories:
            return
        query = "INSERT INTO test.repositories (name, owner, stars, watchers, forks, language, updated) VALUES"
        await self._execute_query(query, repositories, BATCH_SIZE)
        logger.info(f"Added {len(repositories)} records to test.repositories")

    async def save_positions(self, positions: list[tuple]):
        """Saves repository position data."""
        if not positions:
            return
        query = "INSERT INTO test.repositories_positions (date, repo, position) VALUES"
        await self._execute_query(query, positions, BATCH_SIZE)
        logger.info(f"Added {len(positions)} records to test.repositories_positions")

    async def save_commits(self, commits: list[tuple]):
        """Saves commit data for authors."""
        if not commits:
            return
        query = "INSERT INTO test.repositories_authors_commits (date, repo, author, commits_num) VALUES"
        await self._execute_query(query, commits, BATCH_SIZE)
        logger.info(f"Added {len(commits)} records to test.repositories_authors_commits")

    async def _execute_query(self, query: str, values: list[tuple], batch_size: int = 1000):
        """Inserts data into ClickHouse using batch processing."""
        try:
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                await self._ch_client.execute(query, *batch)
        except Exception as e:
            logger.error(f"Error while inserting into ClickHouse: {e}", exc_info=True)
            raise

    async def close(self):
        """Closes the session managing requests to ClickHouse."""
        await self._ch_session.close()
