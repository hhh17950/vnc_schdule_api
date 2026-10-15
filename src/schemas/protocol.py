from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class TaskCreate(BaseModel):
    command: list[str] = Field(min_length=1)
    input_path: str | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)


class TaskView(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    status: str
    command: list[str]
    input_path: str | None
    timeout_seconds: int
    current_attempt_id: str | None
    created_at: datetime
    updated_at: datetime
    result_path: str | None
    error: str | None


class TaskPage(BaseModel):
    items: list[TaskView]
    total: int


class WorkerRegister(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    session_id: str
    slot_ids: list[str] = Field(min_length=1)
    capabilities: dict[str, Any] = Field(default_factory=dict)


class SlotView(BaseModel):
    slot_id: str
    status: str
    task_id: str | None = None


class Heartbeat(BaseModel):
    session_id: str
    slots: list[SlotView]


class HeartbeatResult(BaseModel):
    lease_until: datetime
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
    input_path: str | None
    timeout_seconds: int
    deadline_at: datetime


class ResultRequest(BaseModel):
    session_id: str
    attempt_id: str
    status: Literal["succeeded", "failed", "cancelled", "timed_out"]
    slot_clean: bool
    result_path: str | None = None
    error: str | None = None
