from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TaskCreate(BaseModel):
    command: list[str] = Field(min_length=1)
    cwd: str = Field(min_length=1, max_length=1000)
    input_path: str | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)


    @field_validator("cwd")
    @classmethod
    def absolute_cwd(cls, value: str) -> str:
        if not PurePosixPath(value).is_absolute():
            raise ValueError("cwd 必须是 Worker 物理机上的绝对路径")
        return value


class TaskView(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    status: str
    command: list[str]
    cwd: str
    input_path: str | None
    timeout_seconds: int
    current_attempt_id: str | None
    created_at: datetime
    updated_at: datetime
    result_path: str | None
    result_summary: dict[str, Any] | None
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
    attempt_id: str
    command: list[str]
    cwd: str
    input_path: str | None
    deadline_timestamp: float


class ResultRequest(BaseModel):
    session_id: str
    attempt_id: str
    status: Literal["succeeded", "failed", "cancelled", "timed_out"]
    slot_clean: bool
    result_path: str | None = None
    result_summary: dict[str, Any] | None = None
    error: str | None = None
