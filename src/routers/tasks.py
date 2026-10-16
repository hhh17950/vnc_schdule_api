from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_session
from src.models import Task
from src.schemas.protocol import TaskCreate, TaskPage, TaskView
from src.services.tasks import cancel_task, create_task, list_tasks

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])


@router.post("", response_model=TaskView, status_code=status.HTTP_201_CREATED)
async def create(
    payload: TaskCreate, session: AsyncSession = Depends(get_session),
) -> Task:
    return await create_task(session, payload)


@router.get("", response_model=TaskPage)
async def list_all(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> TaskPage:
    rows, total = await list_tasks(session, limit, offset)
    return TaskPage(items=[TaskView.model_validate(row) for row in rows], total=total)


@router.get("/{task_id}", response_model=TaskView)
async def get_one(task_id: str, session: AsyncSession = Depends(get_session)) -> Task:
    task = await session.get(Task, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return task


@router.post("/{task_id}/cancel", response_model=TaskView, status_code=202)
async def cancel(task_id: str, session: AsyncSession = Depends(get_session)) -> Task:
    return await cancel_task(session, task_id)
