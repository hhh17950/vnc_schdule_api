from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import get_settings
from src.core.database import get_session
from src.models import Worker
from src.schemas.protocol import (
    ClaimRequest, ClaimResult, Heartbeat, HeartbeatResult, ResultRequest, WorkerRegister,
    WorkerRegisterResult, WorkerShutdown,
)
from src.services.tasks import claim_task, finish_task
from src.services.workers import heartbeat, register_worker, shutdown_worker, unregister_worker

router = APIRouter(prefix="/api/v1/workers", tags=["workers"])


@router.post("/register", response_model=WorkerRegisterResult)
async def register(payload: WorkerRegister, session: AsyncSession = Depends(get_session)) -> WorkerRegisterResult:
    worker = await register_worker(session, payload)
    return WorkerRegisterResult(
        worker_id=worker.id,
        session_id=worker.session_id,
        lease_seconds=get_settings().lease_seconds,
    )


@router.post("/{worker_id}/heartbeat", response_model=HeartbeatResult)
async def beat(worker_id: str, payload: Heartbeat, session: AsyncSession = Depends(get_session)) -> HeartbeatResult:
    return await heartbeat(session, worker_id, payload)


@router.post("/{worker_id}/shutdown")
async def shutdown(worker_id: str, payload: WorkerShutdown, session: AsyncSession = Depends(get_session)) -> dict[str, str]:
    await shutdown_worker(session, worker_id, payload.session_id)
    return {"status": "stopped"}


@router.post("/{worker_id}/unregister")
async def unregister(worker_id: str, session: AsyncSession = Depends(get_session)) -> dict[str, str]:
    await unregister_worker(session, worker_id)
    return {"status": "stopped"}


@router.post("/{worker_id}/claim", response_model=ClaimResult)
async def claim(worker_id: str, payload: ClaimRequest, session: AsyncSession = Depends(get_session)) -> ClaimResult:
    return await claim_task(session, worker_id, payload)


@router.post("/{worker_id}/tasks/{task_id}/result")
async def result(
    worker_id: str, task_id: str, payload: ResultRequest,
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    task = await finish_task(session, worker_id, task_id, payload)
    return {"task_id": task.id, "status": task.status}


@router.get("")
async def list_workers(session: AsyncSession = Depends(get_session)) -> list[dict[str, object]]:
    now = datetime.now(timezone.utc)
    rows = await session.scalars(select(Worker).order_by(Worker.id))
    return [{
        "worker_id": row.id, "online": row.lease_until > now,
        "session_id": row.session_id,
        "lease_until": row.lease_until.isoformat(),
        "slots": row.slots if row.lease_until > now else [],
    } for row in rows]
