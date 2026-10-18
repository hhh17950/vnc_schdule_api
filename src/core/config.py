from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# 仓库根目录（src/core/config.py 往上两级）。.env 固定从仓库根目录读取，
# 这样无论从哪个工作目录启动（uvicorn、PyCharm、systemd）都能找到配置，
# 同时不把某台机器的绝对路径写死进仓库。
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="VNC_", env_file=ENV_FILE, extra="ignore",
    )

    database_url: str
    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_prefix: str = "vnc_schedule"
    task_timeout_seconds: int = Field(default=7200, ge=1)
    lease_seconds: int = Field(default=30, ge=5)
    log_dir: Path = Path("logs")
    log_timezone: str = "Asia/Shanghai"
    log_retention_days: int = 30
    outbox_interval_seconds: int = 2
    file_mgmt_url: str | None = None

    @property
    def stream(self) -> str:
        return f"{self.redis_prefix}:tasks"

    @property
    def group(self) -> str:
        return f"{self.redis_prefix}:workers"


@lru_cache
def get_settings() -> Settings:
    return Settings()
