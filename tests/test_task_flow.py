import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from src.core.database import session_factory
from src.core.config import get_settings
from src.models import OutboxEvent, Task, TaskAttempt, Worker
from src.schemas.protocol import ClaimRequest, Heartbeat, ResultRequest, TaskCreate, WorkerRegister
from src.services.dispatch import dispatch_once, reconcile_once, recover_pending_once
from src.services.tasks import cancel_task, claim_task, create_task, finish_task, utcnow
from src.services.workers import heartbeat, register_worker, shutdown_worker


@pytest.mark.asyncio
async def test_create_outbox_claim_and_finish() -> None:
    worker_id = f"test-{uuid4()}"
    session_id = str(uuid4())
    async with session_factory() as db:
        await register_worker(db, WorkerRegister(worker_id=worker_id, session_id=session_id, slot_ids=["user1"]))
        task = await create_task(db, TaskCreate(command=["/bin/true"]), str(uuid4()))
        assert task.timeout_seconds == 7200
        event = await db.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task.id))
        assert event is not None
        claim = await claim_task(db, worker_id, ClaimRequest(
            session_id=session_id, slot_id="user1", task_id=task.id,
        ))
        again = await claim_task(db, worker_id, ClaimRequest(
            session_id=session_id, slot_id="user1", task_id=task.id,
        ))
        assert again.attempt_id == claim.attempt_id
        result = ResultRequest(
            session_id=session_id, attempt_id=claim.attempt_id,
            status="succeeded", slot_clean=True, result_path="result/task.log",
        )
        done = await finish_task(db, worker_id, task.id, result)
        assert done.status == "succeeded"
        assert (await finish_task(db, worker_id, task.id, result)).status == "succeeded"


@pytest.mark.asyncio
async def test_cancel_queued_prevents_claim() -> None:
    worker_id = f"test-{uuid4()}"
    session_id = str(uuid4())
    async with session_factory() as db:
        await register_worker(db, WorkerRegister(worker_id=worker_id, session_id=session_id, slot_ids=["user1"]))
        task = await create_task(db, TaskCreate(command=["/bin/true"]), str(uuid4()))
        assert (await cancel_task(db, task.id)).status == "cancelled"
        with pytest.raises(HTTPException) as error:
            await claim_task(db, worker_id, ClaimRequest(
                session_id=session_id, slot_id="user1", task_id=task.id,
            ))
        assert error.value.status_code == 409


@pytest.mark.asyncio
async def test_unregistered_slot_cannot_claim() -> None:
    worker_id = f"test-{uuid4()}"
    session_id = str(uuid4())
    async with session_factory() as db:
        await register_worker(db, WorkerRegister(
            worker_id=worker_id, session_id=session_id, slot_ids=["user1"],
        ))
        task = await create_task(db, TaskCreate(command=["/bin/true"]), str(uuid4()))
        with pytest.raises(HTTPException) as error:
            await claim_task(db, worker_id, ClaimRequest(
                session_id=session_id, slot_id="user2", task_id=task.id,
            ))
        assert error.value.status_code == 409


@pytest.mark.asyncio
async def test_idempotency_key() -> None:
    key = str(uuid4())
    async with session_factory() as db:
        first = await create_task(db, TaskCreate(command=["/bin/true"]), key)
        repeated = await create_task(db, TaskCreate(command=["/bin/true"]), key)
        assert first.id == repeated.id
        with pytest.raises(HTTPException):
            await create_task(db, TaskCreate(command=["/bin/false"]), key)


@pytest.mark.asyncio
async def test_concurrent_idempotency_key_creates_one_task() -> None:
    key = str(uuid4())

    async def create_once() -> str:
        async with session_factory() as db:
            task = await create_task(db, TaskCreate(command=["/bin/true"]), key)
            return task.id

    first, second = await asyncio.gather(create_once(), create_once())
    assert first == second
    async with session_factory() as db:
        rows = (await db.scalars(select(OutboxEvent).where(OutboxEvent.task_id == first))).all()
        assert len(rows) == 1


@pytest.mark.asyncio
async def test_outbox_retries_after_redis_failure() -> None:
    class BrokenRedis:
        async def xadd(self, stream: str, values: dict[str, str]) -> None:
            raise ConnectionError("simulated Redis outage")

    async with session_factory() as db:
        task = await create_task(db, TaskCreate(command=["/bin/true"]), str(uuid4()))
    await dispatch_once(BrokenRedis(), get_settings())
    async with session_factory() as db:
        event = await db.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task.id))
        assert event is not None
        assert event.sent_at is None
        assert event.attempts >= 1


@pytest.mark.asyncio
async def test_abandoned_pending_message_is_requeued() -> None:
    class PendingRedis:
        def __init__(self, task_id: str) -> None:
            self.task_id = task_id
            self.acked = False

        async def xpending_range(self, stream: str, group: str, minimum: str,
                                 maximum: str, count: int) -> list[dict]:
            return [{"message_id": "1-0", "time_since_delivered": 60000}]

        async def xrange(self, stream: str, min: str, max: str) -> list[tuple[str, dict]]:
            return [("1-0", {"task_id": self.task_id})]

        async def xack(self, stream: str, group: str, message_id: str) -> None:
            self.acked = True

    async with session_factory() as db:
        task = await create_task(db, TaskCreate(command=["/bin/true"]), str(uuid4()))
        event = await db.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task.id))
        event.sent_at = task.created_at
        await db.commit()
    redis = PendingRedis(task.id)
    await recover_pending_once(redis, get_settings())
    assert redis.acked
    async with session_factory() as db:
        events = (await db.scalars(select(OutboxEvent).where(OutboxEvent.task_id == task.id))).all()
        assert len(events) == 2
        assert sum(event.sent_at is None for event in events) == 1


@pytest.mark.asyncio
async def test_old_session_cannot_overwrite_lost_task() -> None:
    worker_id = f"test-{uuid4()}"
    old_session = str(uuid4())
    async with session_factory() as db:
        await register_worker(db, WorkerRegister(worker_id=worker_id, session_id=old_session, slot_ids=["user1"]))
        task = await create_task(db, TaskCreate(command=["/bin/true"]), str(uuid4()))
        claim = await claim_task(db, worker_id, ClaimRequest(
            session_id=old_session, slot_id="user1", task_id=task.id,
        ))
        await shutdown_worker(db, worker_id, old_session)
        await register_worker(db, WorkerRegister(worker_id=worker_id, session_id=str(uuid4()), slot_ids=["user1"]))
        result = await finish_task(db, worker_id, task.id, ResultRequest(
            session_id=old_session, attempt_id=claim.attempt_id,
            status="succeeded", slot_clean=True,
        ))
        assert result.status == "lost"


@pytest.mark.asyncio
async def test_heartbeat_repeats_cancel_and_does_not_extend_deadline() -> None:
    worker_id = f"test-{uuid4()}"
    session_id = str(uuid4())
    async with session_factory() as db:
        await register_worker(db, WorkerRegister(worker_id=worker_id, session_id=session_id, slot_ids=["user1"]))
        task = await create_task(db, TaskCreate(command=["/bin/sleep", "30"]), str(uuid4()))
        claim = await claim_task(db, worker_id, ClaimRequest(
            session_id=session_id, slot_id="user1", task_id=task.id,
        ))
        await cancel_task(db, task.id)
        pulse = Heartbeat(session_id=session_id, slots=[])
        assert task.id in (await heartbeat(db, worker_id, pulse)).cancel_task_ids
        assert task.id in (await heartbeat(db, worker_id, pulse)).cancel_task_ids
        attempt = await db.get(TaskAttempt, claim.attempt_id)
        assert attempt.deadline_at == claim.deadline_at
        with pytest.raises(HTTPException) as error:
            await finish_task(db, worker_id, task.id, ResultRequest(
                session_id=session_id, attempt_id=claim.attempt_id,
                status="succeeded", slot_clean=True,
            ))
        assert error.value.status_code == 409
        await finish_task(db, worker_id, task.id, ResultRequest(
            session_id=session_id, attempt_id=claim.attempt_id,
            status="cancelled", slot_clean=True,
        ))
        assert task.id not in (await heartbeat(db, worker_id, pulse)).cancel_task_ids


@pytest.mark.asyncio
async def test_expired_lease_marks_execution_unknown() -> None:
    worker_id = f"test-{uuid4()}"
    session_id = str(uuid4())
    async with session_factory() as db:
        await register_worker(db, WorkerRegister(worker_id=worker_id, session_id=session_id, slot_ids=["user1"]))
        task = await create_task(db, TaskCreate(command=["/bin/sleep", "30"]), str(uuid4()))
        claim = await claim_task(db, worker_id, ClaimRequest(
            session_id=session_id, slot_id="user1", task_id=task.id,
        ))
        worker = await db.get(Worker, worker_id)
        worker.lease_until = utcnow() - timedelta(seconds=1)
        await db.commit()
    await reconcile_once(get_settings())
    async with session_factory() as db:
        task = await db.get(Task, task.id)
        attempt = await db.get(TaskAttempt, claim.attempt_id)
        assert task.status == "lost"
        assert attempt.status == "lost"


@pytest.mark.asyncio
async def test_reconcile_recreates_notification_after_redis_loss() -> None:
    async with session_factory() as db:
        task = await create_task(db, TaskCreate(command=["/bin/true"]), str(uuid4()))
        old = utcnow() - timedelta(minutes=5)
        task.created_at = old
        task.last_enqueued_at = old
        event = await db.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task.id))
        event.sent_at = old
        await db.commit()
    await reconcile_once(get_settings())
    async with session_factory() as db:
        events = (await db.scalars(select(OutboxEvent).where(
            OutboxEvent.task_id == task.id,
        ).order_by(OutboxEvent.generation))).all()
        assert len(events) == 2
        assert events[-1].sent_at is None
        assert events[-1].generation == 2
