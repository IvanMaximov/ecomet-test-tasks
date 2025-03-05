import os

GITHUB_ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN", "")

# Maximum number of repositories to collect.
REPOSITORY_LIMIT = int(os.getenv("REPOSITORY_LIMIT", "100"))

# Request limits on the number and frequency of API calls.
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
REQUESTS_PER_SECOND = int(os.getenv("REQUESTS_PER_SECOND", "5"))

# ClickHouse database configuration.
CLICKHOUSE_DSN = os.getenv("CLICKHOUSE_DSN", "")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# Number of records to be inserted into the ClickHouse table at a time.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
