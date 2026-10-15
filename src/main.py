import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
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


static_dir = Path(__file__).resolve().parent / "static"

# 默认文档页面引用公网 CDN；内网环境改为随服务发布的固定版本资源。
app = FastAPI(
    title="VNC Schedule API",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)
app.mount("/static", StaticFiles(directory=static_dir), name="static")
app.include_router(task_router)
app.include_router(worker_router)


@app.get("/docs", include_in_schema=False)
async def swagger_ui() -> HTMLResponse:
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} - Swagger UI",
        swagger_js_url="/static/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui.css",
        swagger_favicon_url="/static/favicon-32x32.png",
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(Exception)
async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("request failed path={}", request.url.path)
    return JSONResponse(status_code=500, content={"detail": "internal server error"})
