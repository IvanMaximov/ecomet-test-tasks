from typing import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
import uvicorn
from fastapi import FastAPI, APIRouter, Depends, Request, HTTPException
from typing_extensions import Annotated

from config import get_settings
from logging_presets import setup_logging

logger = setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the connection pool lifecycle."""
    settings = get_settings()
    logger.info("Initializing the connection pool...")

    try:
        app.state.pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size
        )
        logger.info("Connection pool successfully created.")
        yield
    except Exception as e:
        logger.error(f"Error while creating the connection pool: {e}", exc_info=True)
        raise
    finally:
        pool = getattr(app.state, "pool", None)
        if pool:
            await pool.close()
            logger.info("Connection pool closed.")


async def get_pg_connection(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """Retrieves a database connection from the pool."""
    pool: asyncpg.Pool | None = getattr(request.app.state, "pool", None)

    if not pool:
        logger.error("Database connection pool is not available.")
        raise HTTPException(status_code=500, detail="Database connection pool is unavailable")

    try:
        async with pool.acquire() as conn:
            yield conn
    except Exception as e:
        logger.error(f"Error while retrieving the connection: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error while retrieving the connection") from e


async def get_db_version(conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)]) -> str:
    """Fetches the database version."""
    logger.info("Requesting database version...")

    try:
        version = await conn.fetchval("SELECT version()")
        logger.info(f"Database version: {version}")
        return version
    except Exception as e:
        logger.error(f"Error while retrieving the database version: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error while retrieving the database version") from e


def create_app() -> FastAPI:
    """Creates and configures the FastAPI app."""
    app = FastAPI(title="e-Comet", lifespan=lifespan)

    router = APIRouter(prefix="/api")
    router.add_api_route("/db_version", get_db_version, methods=["GET"])
    app.include_router(router)

    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
