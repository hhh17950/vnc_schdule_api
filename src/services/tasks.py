from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import get_settings
from src.models import OutboxEvent, Task, TaskAttempt, Worker
from src.schemas.protocol import ClaimRequest, ClaimResult, ResultRequest, TaskCreate


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def task_payload_equal(task: Task, payload: TaskCreate) -> bool:
    return (
        task.command == payload.command
        and task.input_path == payload.input_path
        and task.timeout_seconds == (payload.timeout_seconds or get_settings().task_timeout_seconds)
    )


async def create_task(session: AsyncSession, payload: TaskCreate, key: str | None) -> Task:
    if key:
        existing = await session.scalar(select(Task).where(Task.idempotency_key == key))
        if existing:
            if not task_payload_equal(existing, payload):
                raise HTTPException(status_code=409, detail="idempotency key already used")
            return existing
    now = utcnow()
    task = Task(
        command=payload.command, input_path=payload.input_path,
        timeout_seconds=payload.timeout_seconds or get_settings().task_timeout_seconds,
        idempotency_key=key, created_at=now, updated_at=now,
    )
    session.add(task)
    try:
        await session.flush()
    except IntegrityError:
        # 并发请求可能同时查不到幂等键；唯一约束决定胜者，再读取已提交任务。
        await session.rollback()
        if key:
            existing = await session.scalar(select(Task).where(Task.idempotency_key == key))
            if existing:
                if not task_payload_equal(existing, payload):
                    raise HTTPException(status_code=409, detail="idempotency key already used")
                return existing
        raise
    # 任务和 Outbox 在同一事务提交；即使 Redis 暂时不可用也不会丢失待投递任务。
    session.add(OutboxEvent(task_id=task.id, created_at=now, next_attempt_at=now))
    await session.commit()
    await session.refresh(task)
    return task


async def list_tasks(session: AsyncSession, limit: int, offset: int) -> tuple[list[Task], int]:
    rows = await session.scalars(select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset))
    total = await session.scalar(select(func.count()).select_from(Task))
    return list(rows), int(total or 0)


async def cancel_task(session: AsyncSession, task_id: str) -> Task:
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    if task.status == "queued":
        task.status = "cancelled"
    elif task.status == "running":
        task.status = "cancel_requested"
    task.updated_at = utcnow()
    await session.commit()
    return task


async def claim_task(session: AsyncSession, worker_id: str, payload: ClaimRequest) -> ClaimResult:
    now = utcnow()
    worker = await session.scalar(select(Worker).where(Worker.id == worker_id).with_for_update())
    if worker is None or worker.session_id != payload.session_id or worker.lease_until <= now:
        raise HTTPException(status_code=409, detail="worker session expired")
    if payload.slot_id not in worker.capabilities.get("slot_ids", []):
        raise HTTPException(status_code=409, detail="slot is not registered")
    task = await session.scalar(select(Task).where(Task.id == payload.task_id).with_for_update())
    if task and task.status == "running" and task.current_attempt_id:
        # 授权响应丢失时，同一 Session/Slot 可拿到原 attempt；不得创建第二次执行。
        previous = await session.get(TaskAttempt, task.current_attempt_id)
        if previous and previous.worker_id == worker_id and previous.session_id == payload.session_id and previous.slot_id == payload.slot_id:
            return ClaimResult(
                attempt_id=previous.id, command=task.command, input_path=task.input_path,
                timeout_seconds=task.timeout_seconds, deadline_at=previous.deadline_at,
            )
    if task is None or task.status != "queued":
        raise HTTPException(status_code=409, detail="task is not claimable")
    occupied = await session.scalar(
        select(TaskAttempt.id).where(
            TaskAttempt.worker_id == worker_id,
            TaskAttempt.session_id == payload.session_id,
            TaskAttempt.slot_id == payload.slot_id,
            TaskAttempt.finished_at.is_(None),
        )
    )
    if occupied:
        raise HTTPException(status_code=409, detail="slot is occupied")
    attempt = TaskAttempt(
        task_id=task.id, worker_id=worker_id, session_id=payload.session_id,
        slot_id=payload.slot_id, started_at=now,
        deadline_at=now + timedelta(seconds=task.timeout_seconds),
    )
    session.add(attempt)
    await session.flush()
    task.status = "running"
    task.current_attempt_id = attempt.id
    task.updated_at = now
    await session.commit()
    return ClaimResult(
        attempt_id=attempt.id, command=task.command, input_path=task.input_path,
        timeout_seconds=task.timeout_seconds, deadline_at=attempt.deadline_at,
    )


async def finish_task(session: AsyncSession, worker_id: str, task_id: str, payload: ResultRequest) -> Task:
    now = utcnow()
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    attempt = await session.scalar(select(TaskAttempt).where(TaskAttempt.id == payload.attempt_id).with_for_update())
    if attempt is None or attempt.task_id != task_id or attempt.worker_id != worker_id:
        raise HTTPException(status_code=409, detail="attempt mismatch")
    if attempt.finished_at is not None:
        return task
    worker = await session.get(Worker, worker_id)
    if worker is None or worker.session_id != payload.session_id or attempt.session_id != payload.session_id:
        raise HTTPException(status_code=409, detail="stale worker session")
    if worker.lease_until <= now:
        raise HTTPException(status_code=409, detail="worker lease expired")
    if task.current_attempt_id != attempt.id or task.status not in {
        "running", "cancel_requested", "timeout_requested"
    }:
        raise HTTPException(status_code=409, detail="task state changed")
    if task.status == "cancel_requested" and payload.status != "cancelled":
        # 取消与正常完成竞争时，以持有任务行锁后看到的数据库状态为准。
        raise HTTPException(status_code=409, detail="cancellation wins")
    if task.status == "timeout_requested" and payload.status != "timed_out":
        raise HTTPException(status_code=409, detail="timeout wins")
    task.status = payload.status if payload.slot_clean else "lost"
    task.updated_at = now
    task.result_path = payload.result_path
    task.result_summary = payload.result_summary
    task.error = payload.error
    attempt.status = task.status
    attempt.finished_at = now
    attempt.result_path = payload.result_path
    attempt.result_summary = payload.result_summary
    attempt.error = payload.error
    await session.commit()
    return task
