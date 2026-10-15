from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_session
from src.core.security import require_worker
from src.models import Worker
from src.schemas.protocol import (
    ClaimRequest, ClaimResult, Heartbeat, HeartbeatResult, ResultRequest, WorkerRegister,
    WorkerShutdown,
)
from src.services.tasks import claim_task, finish_task
from src.services.workers import heartbeat, register_worker, shutdown_worker

router = APIRouter(prefix="/api/v1/workers", tags=["workers"])


@router.post("/register", dependencies=[Depends(require_worker)])
async def register(payload: WorkerRegister, session: AsyncSession = Depends(get_session)) -> dict[str, str]:
    worker = await register_worker(session, payload)
    return {"worker_id": worker.id, "session_id": worker.session_id}


@router.post("/{worker_id}/heartbeat", response_model=HeartbeatResult, dependencies=[Depends(require_worker)])
async def beat(worker_id: str, payload: Heartbeat, session: AsyncSession = Depends(get_session)) -> HeartbeatResult:
    return await heartbeat(session, worker_id, payload)


@router.post("/{worker_id}/shutdown", dependencies=[Depends(require_worker)])
async def shutdown(worker_id: str, payload: WorkerShutdown, session: AsyncSession = Depends(get_session)) -> dict[str, str]:
    await shutdown_worker(session, worker_id, payload.session_id)
    return {"status": "stopped"}


@router.post("/{worker_id}/claim", response_model=ClaimResult, dependencies=[Depends(require_worker)])
async def claim(worker_id: str, payload: ClaimRequest, session: AsyncSession = Depends(get_session)) -> ClaimResult:
    return await claim_task(session, worker_id, payload)


@router.post("/{worker_id}/tasks/{task_id}/result", dependencies=[Depends(require_worker)])
async def result(
    worker_id: str, task_id: str, payload: ResultRequest,
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    task = await finish_task(session, worker_id, task_id, payload)
    return {"task_id": task.id, "status": task.status}


@router.get("", dependencies=[Depends(require_worker)])
async def list_workers(session: AsyncSession = Depends(get_session)) -> list[dict[str, object]]:
    now = datetime.now(timezone.utc)
    rows = await session.scalars(select(Worker).order_by(Worker.id))
    return [{
        "worker_id": row.id, "online": row.lease_until > now,
        "session_id": row.session_id,
        "lease_until": row.lease_until.isoformat(),
        "slots": row.slots if row.lease_until > now else [],
    } for row in rows]
