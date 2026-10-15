import asyncio
import time
from datetime import timedelta

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import select

from src.core.config import Settings
from src.core.database import session_factory
from src.models import OutboxEvent, Task, TaskAttempt, Worker
from src.services.tasks import utcnow


async def dispatch_once(redis: Redis, settings: Settings) -> int:
    now = utcnow()
    sent = 0
    async with session_factory() as session:
        rows = await session.scalars(
            select(OutboxEvent).where(
                OutboxEvent.sent_at.is_(None), OutboxEvent.next_attempt_at <= now
            ).order_by(OutboxEvent.created_at).limit(20).with_for_update(skip_locked=True)
        )
        for event in rows:
            task = await session.get(Task, event.task_id)
            if task is None or task.status != "queued":
                event.sent_at = now
                continue
            try:
                # 先写 Stream，后记 sent_at；中间崩溃会重复投递，由数据库授权去重。
                await redis.xadd(settings.stream, {
                    "task_id": event.task_id, "event_id": event.id,
                    "generation": str(event.generation), "version": "1",
                })
            except Exception:
                event.attempts += 1
                event.next_attempt_at = now + timedelta(seconds=min(60, 2 ** min(event.attempts, 6)))
                logger.exception("outbox delivery failed task_id={}", event.task_id)
            else:
                event.sent_at = now
                task.last_enqueued_at = now
                sent += 1
        await session.commit()
    return sent


async def reconcile_once(settings: Settings) -> None:
    now = utcnow()
    async with session_factory() as session:
        workers = await session.scalars(select(Worker).where(Worker.lease_until <= now).with_for_update(skip_locked=True))
        for worker in workers:
            attempts = await session.scalars(select(TaskAttempt).where(
                TaskAttempt.worker_id == worker.id,
                TaskAttempt.session_id == worker.session_id,
                TaskAttempt.finished_at.is_(None),
            ).with_for_update(skip_locked=True))
            for attempt in attempts:
                task = await session.get(Task, attempt.task_id, with_for_update=True)
                if task and task.current_attempt_id == attempt.id and task.status in {
                    "running", "cancel_requested", "timeout_requested"
                }:
                    task.status = "lost"
                    task.updated_at = now
                    task.error = "worker lease expired; execution result unknown"
                    attempt.status = "lost"
                    attempt.finished_at = now
            worker.slots = []
        overdue = await session.scalars(select(TaskAttempt).where(
            TaskAttempt.deadline_at <= now, TaskAttempt.finished_at.is_(None)
        ).with_for_update(skip_locked=True))
        for attempt in overdue:
            task = await session.get(Task, attempt.task_id, with_for_update=True)
            if task and task.current_attempt_id == attempt.id and task.status == "running":
                task.status = "timeout_requested"
                task.updated_at = now
        queued = await session.scalars(select(Task).where(
            Task.status == "queued",
            Task.created_at <= now - timedelta(seconds=120),
        ).with_for_update(skip_locked=True))
        for task in queued:
            if task.last_enqueued_at and task.last_enqueued_at > now - timedelta(seconds=120):
                continue
            pending = await session.scalar(select(OutboxEvent.id).where(
                OutboxEvent.task_id == task.id, OutboxEvent.sent_at.is_(None)
            ))
            if pending:
                continue
            last = await session.scalar(select(OutboxEvent.generation).where(
                OutboxEvent.task_id == task.id
            ).order_by(OutboxEvent.generation.desc()).limit(1))
            session.add(OutboxEvent(
                task_id=task.id, generation=(last or 0) + 1,
                created_at=now, next_attempt_at=now,
            ))
            task.last_enqueued_at = now
        await session.commit()


async def recover_pending_once(redis: Redis, settings: Settings) -> None:
    """Resolve abandoned PEL entries after checking the authoritative task state."""
    cursor = "-"
    while True:
        entries = await redis.xpending_range(settings.stream, settings.group, cursor, "+", 100)
        if not entries:
            return
        for entry in entries:
            message_id = entry["message_id"]
            cursor = f"({message_id}"
            if entry["time_since_delivered"] < settings.lease_seconds * 1000:
                continue
            messages = await redis.xrange(settings.stream, min=message_id, max=message_id)
            if not messages:
                await redis.xack(settings.stream, settings.group, message_id)
                continue
            task_id = messages[0][1].get("task_id")
            async with session_factory() as session:
                task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
                if task is None:
                    await redis.xack(settings.stream, settings.group, message_id)
                    continue
                if task.status == "queued":
                    # PEL 中的旧消息只为未授权任务补发；运行中的任务绝不能直接重跑。
                    pending = await session.scalar(select(OutboxEvent.id).where(
                        OutboxEvent.task_id == task.id, OutboxEvent.sent_at.is_(None)
                    ))
                    if not pending:
                        last = await session.scalar(select(OutboxEvent.generation).where(
                            OutboxEvent.task_id == task.id
                        ).order_by(OutboxEvent.generation.desc()).limit(1))
                        now = utcnow()
                        session.add(OutboxEvent(
                            task_id=task.id, generation=(last or 0) + 1,
                            created_at=now, next_attempt_at=now,
                        ))
                        task.last_enqueued_at = now
                    await session.commit()
                    await redis.xack(settings.stream, settings.group, message_id)
                elif task.status in {"succeeded", "failed", "cancelled", "timed_out", "lost"}:
                    await redis.xack(settings.stream, settings.group, message_id)
        if len(entries) < 100:
            return


async def run_dispatch(redis: Redis, settings: Settings) -> None:
    next_reconcile = 0.0
    while True:
        try:
            try:
                await redis.xgroup_create(settings.stream, settings.group, id="0", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise
            await dispatch_once(redis, settings)
            if time.monotonic() >= next_reconcile:
                await reconcile_once(settings)
                await recover_pending_once(redis, settings)
                next_reconcile = time.monotonic() + 10
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("dispatcher cycle failed")
        await asyncio.sleep(settings.outbox_interval_seconds)
