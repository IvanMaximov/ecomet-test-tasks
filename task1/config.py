from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    pool_min_size: int = 2
    pool_max_size: int = 10

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()
