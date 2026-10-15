import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from loguru import logger
from redis.asyncio import Redis

from src.core.config import get_settings
from src.core.database import engine
from src.core.logging import configure_logging
from src.routers.tasks import router as task_router
from src.routers.workers import router as worker_router
from src.services.dispatch import run_dispatch


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    runner = asyncio.create_task(run_dispatch(redis, settings))
    try:
        yield
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        await redis.aclose()
        await engine.dispose()


app = FastAPI(title="VNC Schedule API", lifespan=lifespan)
app.include_router(task_router)
app.include_router(worker_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(Exception)
async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("request failed path={}", request.url.path)
    return JSONResponse(status_code=500, content={"detail": "internal server error"})
