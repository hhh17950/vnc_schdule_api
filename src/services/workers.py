from datetime import timedelta

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import get_settings
from src.models import Task, TaskAttempt, Worker
from src.schemas.protocol import Heartbeat, HeartbeatResult, WorkerRegister
from src.services.tasks import utcnow


async def _mark_worker_offline(session: AsyncSession, worker: Worker, reason: str) -> None:
    """将 Worker 下线，并把无法确认结果的执行标记为 lost。"""
    now = utcnow()
    attempts = await session.scalars(select(TaskAttempt).where(
        TaskAttempt.worker_id == worker.id,
        TaskAttempt.session_id == worker.session_id,
        TaskAttempt.finished_at.is_(None),
    ).with_for_update())
    for attempt in attempts:
        task = await session.get(Task, attempt.task_id, with_for_update=True)
        attempt.status = "lost"
        attempt.finished_at = now
        if task and task.current_attempt_id == attempt.id and task.status in {
            "running", "cancel_requested", "timeout_requested"
        }:
            task.status = "lost"
            task.error = reason
            task.updated_at = now
    worker.lease_until = now
    worker.updated_at = now
    worker.slots = []


async def register_worker(session: AsyncSession, payload: WorkerRegister) -> Worker:
    now = utcnow()
    worker = await session.scalar(select(Worker).where(Worker.id == payload.worker_id).with_for_update())
    if worker and worker.lease_until > now and worker.session_id != payload.session_id:
        raise HTTPException(status_code=409, detail="worker already active")
    if worker and worker.session_id != payload.session_id:
        # 新 Session 接管稳定 worker_id 时，旧执行的结果未知，不自动补跑。
        await _mark_worker_offline(
            session, worker,
            "worker started a new session; prior execution result unknown",
        )
    if worker is None:
        worker = Worker(id=payload.worker_id, session_id=payload.session_id)
        session.add(worker)
    worker.session_id = payload.session_id
    worker.lease_until = now + timedelta(seconds=get_settings().lease_seconds)
    worker.updated_at = now
    worker.slot_ids = payload.slot_ids
    worker.slots = []
    await session.commit()
    return worker


async def heartbeat(session: AsyncSession, worker_id: str, payload: Heartbeat) -> HeartbeatResult:
    now = utcnow()
    worker = await session.scalar(select(Worker).where(Worker.id == worker_id).with_for_update())
    if worker is None or worker.session_id != payload.session_id or worker.lease_until <= now:
        raise HTTPException(status_code=409, detail="worker session expired")
    worker.lease_until = now + timedelta(seconds=get_settings().lease_seconds)
    worker.updated_at = now
    worker.slots = [slot.model_dump() for slot in payload.slots]
    rows = await session.scalars(
        # 未完成的取消/超时请求每次心跳都返回，直到结果确认落库。
        select(Task.id).join(TaskAttempt, Task.current_attempt_id == TaskAttempt.id).where(
            TaskAttempt.worker_id == worker_id,
            TaskAttempt.session_id == payload.session_id,
            Task.status.in_(["cancel_requested", "timeout_requested"]),
        )
    )
    cancellations = list(rows)
    await session.commit()
    return HeartbeatResult(cancel_task_ids=cancellations)


async def shutdown_worker(session: AsyncSession, worker_id: str, session_id: str) -> None:
    worker = await session.scalar(select(Worker).where(Worker.id == worker_id).with_for_update())
    if worker is None or worker.session_id != session_id:
        raise HTTPException(status_code=409, detail="worker session changed")
    await _mark_worker_offline(
        session, worker, "worker shut down before reporting a final result",
    )
    await session.commit()


async def unregister_worker(session: AsyncSession, worker_id: str) -> None:
    worker = await session.scalar(select(Worker).where(Worker.id == worker_id).with_for_update())
    if worker is None:
        raise HTTPException(status_code=404, detail="worker not found")
    await _mark_worker_offline(
        session, worker, "worker was forcibly unregistered; execution result unknown",
    )
    await session.commit()
