"""数据库 ORM 模型。"""
from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def new_id() -> str:
    return str(uuid4())


class Base(DeclarativeBase):
    pass


class Task(Base):
    """调度任务；执行字段直接落在任务行上，一个任务只执行一次。"""

    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    command: Mapped[list[str]] = mapped_column(JSON)
    cwd: Mapped[str] = mapped_column(String(1000))
    status: Mapped[str] = mapped_column(String(30), default="queued", index=True)
    timeout_seconds: Mapped[int] = mapped_column(Integer)
    worker_id: Mapped[str | None] = mapped_column(String(120))
    session_id: Mapped[str | None] = mapped_column(String(36))
    slot_id: Mapped[str | None] = mapped_column(String(120))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    job_info: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)


class OutboxEvent(Base):
    """Outbox 事件：任务创建后待投递到 Redis Stream 的通知。"""

    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_outbox_due", "sent_at", "next_attempt_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"))
    generation: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Worker(Base):
    """Worker 大盘信息：仅用于展示，不参与任务授权。"""

    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(36))
    lease_until: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    slot_ids: Mapped[list[str]] = mapped_column(JSON, default=list)
    slots: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
