"""Worker 与 API 之间的接口协议模型。"""
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TaskCreate(BaseModel):
    """创建任务的请求体。"""

    command: list[str] = Field(min_length=1)
    cwd: str = Field(min_length=1, max_length=1000)
    timeout_seconds: int | None = Field(default=None, ge=1)

    @field_validator("cwd")
    @classmethod
    def absolute_cwd(cls, value: str) -> str:
        """cwd 必须是 Worker 物理机上的绝对路径。"""
        if not PurePosixPath(value).is_absolute():
            raise ValueError("cwd 必须是 Worker 物理机上的绝对路径")
        return value


class JobInfo(BaseModel):
    """任务日志信息：文件路径与上传后的 file_management 文件 id。"""

    stdout_path: str | None = None
    stderr_path: str | None = None
    stdout_id: str | None = None
    stderr_id: str | None = None


class TaskView(BaseModel):
    """任务查询视图。"""

    model_config = ConfigDict(from_attributes=True)
    id: str
    status: str
    command: list[str]
    cwd: str
    timeout_seconds: int
    created_at: datetime
    updated_at: datetime
    job_info: JobInfo | None = None
    error: str | None


class TaskPage(BaseModel):
    items: list[TaskView]
    total: int


class WorkerRegister(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    session_id: str
    slot_ids: list[str] = Field(min_length=1)


class WorkerRegisterResult(BaseModel):
    worker_id: str
    session_id: str
    lease_seconds: int


class SlotView(BaseModel):
    slot_id: str
    status: str
    task_id: str | None = None


class Heartbeat(BaseModel):
    session_id: str
    slots: list[SlotView]


class HeartbeatResult(BaseModel):
    cancel_task_ids: list[str]


class WorkerShutdown(BaseModel):
    session_id: str


class ClaimRequest(BaseModel):
    session_id: str
    slot_id: str
    task_id: str


class ClaimResult(BaseModel):
    command: list[str]
    cwd: str
    deadline_timestamp: float


class ResultRequest(BaseModel):
    session_id: str
    status: Literal["succeeded", "failed", "cancelled", "timed_out"]
    stdout_path: str | None = None
    stderr_path: str | None = None
    error: str | None = None
