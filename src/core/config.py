from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VNC_", env_file=".env", extra="ignore")

    database_url: str
    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_prefix: str = "vnc_schedule"
    task_timeout_seconds: int = Field(default=7200, ge=1)
    lease_seconds: int = Field(default=30, ge=5)
    log_dir: Path = Path("logs")
    log_timezone: str = "Asia/Shanghai"
    log_retention_days: int = 30
    outbox_interval_seconds: int = 2

    @property
    def stream(self) -> str:
        return f"{self.redis_prefix}:tasks"

    @property
    def group(self) -> str:
        return f"{self.redis_prefix}:workers"


@lru_cache
def get_settings() -> Settings:
    return Settings()
