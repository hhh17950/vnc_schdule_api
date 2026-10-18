"""file_management 文件传输服务客户端。

职责：把 Worker 本机产生的任务日志上传到 file_management 服务，返回服务端分配的文件 id。
当前 HTTP 契约为占位实现：POST {VNC_FILE_MGMT_URL}，multipart 字段 file，响应 {"id": "..."}。
真实接口确定后只需调整 upload 的请求与响应解析。
"""
from pathlib import Path
from typing import Any

import httpx

from src.core.config import get_settings


class FileManagementError(RuntimeError):
    """文件上传失败。"""


class FileManagementClient:
    """基于 HTTP 的文件服务客户端。"""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    async def upload(self, path: Path) -> str:
        """上传单个文件并返回 file_management 分配的文件 id。"""
        if not path.is_file():
            raise FileManagementError(f"file not found: {path}")
        async with httpx.AsyncClient(timeout=60) as client:
            with path.open("rb") as handle:
                response = await client.post(
                    self.base_url,
                    files={"file": (path.name, handle, "application/octet-stream")},
                )
        if response.status_code >= 400:
            raise FileManagementError(f"upload failed: HTTP {response.status_code}")
        data: Any = response.json()
        file_id = data.get("id") if isinstance(data, dict) else None
        if not isinstance(file_id, str) or not file_id:
            raise FileManagementError("upload response missing id")
        return file_id


def get_file_client() -> FileManagementClient | None:
    """按配置返回文件服务客户端；未配置 URL 时返回 None（跳过上传）。"""
    url = get_settings().file_mgmt_url
    if not url:
        return None
    return FileManagementClient(url)
