"""任务创建、授权、结束等核心业务逻辑。"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import HTTPException
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import get_settings
from src.models import OutboxEvent, Task, Worker
from src.schemas.protocol import ClaimRequest, ClaimResult, ResultRequest, TaskCreate
from src.services.file_management import get_file_client


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


TERMINAL_STATES = {"succeeded", "failed", "cancelled", "timed_out", "unknown"}


async def create_task(session: AsyncSession, payload: TaskCreate) -> Task:
    """创建任务，并在同一事务写入 Outbox 事件，保证不会丢失待投递任务。"""
    now = utcnow()
    task = Task(
        command=payload.command,
        cwd=payload.cwd,
        timeout_seconds=payload.timeout_seconds or get_settings().task_timeout_seconds,
        created_at=now,
        updated_at=now,
    )
    session.add(task)
    await session.flush()
    session.add(OutboxEvent(task_id=task.id, created_at=now, next_attempt_at=now))
    await session.commit()
    await session.refresh(task)
    return task


async def list_tasks(session: AsyncSession, limit: int, offset: int) -> tuple[list[Task], int]:
    rows = await session.scalars(
        select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset)
    )
    total = await session.scalar(select(func.count()).select_from(Task))
    return list(rows), int(total or 0)


async def cancel_task(session: AsyncSession, task_id: str) -> Task:
    """接受取消请求：queued 直接取消，running 转为等待 worker 清理。"""
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
    """授权执行：一个任务只允许从 queued 转到 running 一次。"""
    now = utcnow()
    worker = await session.scalar(select(Worker).where(Worker.id == worker_id).with_for_update())
    if worker is None or worker.session_id != payload.session_id or worker.lease_until <= now:
        raise HTTPException(status_code=409, detail="worker session expired")
    if payload.slot_id not in worker.slot_ids:
        raise HTTPException(status_code=409, detail="slot is not registered")

    task = await session.scalar(select(Task).where(Task.id == payload.task_id).with_for_update())
    if task and task.status == "running":
        # claim 响应丢失时的重试：同一 worker/session/slot 拿回原授权，不产生第二次执行。
        if (
            task.worker_id == worker_id
            and task.session_id == payload.session_id
            and task.slot_id == payload.slot_id
        ):
            return ClaimResult(
                command=task.command,
                cwd=task.cwd,
                deadline_timestamp=task.deadline_at.timestamp(),
            )
        raise HTTPException(status_code=409, detail="task already claimed")
    if task is None or task.status != "queued":
        raise HTTPException(status_code=409, detail="task is not claimable")

    # 一个 slot 同时只允许一个未完成任务。
    occupied = await session.scalar(select(Task.id).where(
        Task.worker_id == worker_id,
        Task.session_id == payload.session_id,
        Task.slot_id == payload.slot_id,
        Task.finished_at.is_(None),
    ))
    if occupied:
        raise HTTPException(status_code=409, detail="slot is occupied")

    task.status = "running"
    task.worker_id = worker_id
    task.session_id = payload.session_id
    task.slot_id = payload.slot_id
    task.started_at = now
    task.deadline_at = now + timedelta(seconds=task.timeout_seconds)
    task.updated_at = now
    await session.commit()
    return ClaimResult(
        command=task.command,
        cwd=task.cwd,
        deadline_timestamp=task.deadline_at.timestamp(),
    )


async def finish_task(session: AsyncSession, worker_id: str, task_id: str, payload: ResultRequest) -> Task:
    """记录执行结果；终态后幂等，同步上传日志但不影响结果。"""
    now = utcnow()
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    # 幂等：重复结果回调直接返回当前终态。
    if task.status in TERMINAL_STATES:
        return task
    if task.worker_id != worker_id or task.session_id != payload.session_id:
        raise HTTPException(status_code=409, detail="stale worker session")
    if task.status not in {"running", "cancel_requested", "timeout_requested"}:
        raise HTTPException(status_code=409, detail="task state changed")
    if task.status == "cancel_requested" and payload.status != "cancelled":
        raise HTTPException(status_code=409, detail="cancellation wins")
    if task.status == "timeout_requested" and payload.status != "timed_out":
        raise HTTPException(status_code=409, detail="timeout wins")

    task.status = payload.status
    task.updated_at = now
    task.finished_at = now
    task.error = payload.error
    task.job_info = {
        "stdout_path": payload.stdout_path,
        "stderr_path": payload.stderr_path,
        "stdout_id": None,
        "stderr_id": None,
    }
    await session.commit()
    await session.refresh(task)
    await _upload_job_logs(session, task)
    return task


async def _upload_job_logs(session: AsyncSession, task: Task) -> None:
    """同步上传任务 stdout/stderr；失败只留空 id，不改任务终态，也不补传。"""
    client = get_file_client()
    info = dict(task.job_info or {})
    changed = False
    for id_key, path_key in (("stdout_id", "stdout_path"), ("stderr_id", "stderr_path")):
        path_str = info.get(path_key)
        if client is None or not path_str:
            continue
        try:
            info[id_key] = await client.upload(Path(path_str))
            changed = True
        except Exception:
            logger.exception("日志上传失败 task_id={} file={}", task.id, path_key)
    if changed:
        task.job_info = info
        await session.commit()
