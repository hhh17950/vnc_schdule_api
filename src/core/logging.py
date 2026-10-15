import os
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from loguru import logger

from .config import get_settings

_file_days: dict[str, date] = {}


def _rotate(message: object, file: object) -> bool:
    settings = get_settings()
    zone = ZoneInfo(settings.log_timezone)
    event_time = getattr(message, "record", {}).get("time")
    day = (event_time or datetime.now(zone)).astimezone(zone).date()
    name = str(file.name)
    if name not in _file_days:
        # 重启时用已有文件的最后修改日期判断是否需要立即跨天轮转。
        modified = Path(name).stat().st_mtime
        _file_days[name] = datetime.fromtimestamp(modified, zone).date()
    change_day = day != _file_days[name]
    _file_days[name] = day
    return change_day or file.tell() + len(str(message).encode()) > 100_000_000


def configure_logging() -> None:
    settings = get_settings()
    _file_days.clear()
    Path(settings.log_dir).mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level="INFO", diagnose=False)
    logger.add(
        str(settings.log_dir / f"api-{os.getpid()}-{{time:YYYY-MM-DD}}.log"), rotation=_rotate,
        retention=f"{settings.log_retention_days} days", compression="gz",
        serialize=True, enqueue=True, diagnose=False,
    )
