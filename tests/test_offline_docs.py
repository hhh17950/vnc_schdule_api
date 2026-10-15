import httpx
import pytest

from src.main import app


@pytest.mark.asyncio
async def test_swagger_ui_uses_only_local_static_assets() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test",
    ) as client:
        response = await client.get("/docs")
        assert response.status_code == 200
        assert 'src="/static/swagger-ui-bundle.js"' in response.text
        assert 'href="/static/swagger-ui.css"' in response.text
        assert 'href="/static/favicon-32x32.png"' in response.text
        assert "https://" not in response.text

        javascript = await client.get("/static/swagger-ui-bundle.js")
        stylesheet = await client.get("/static/swagger-ui.css")
        assert javascript.status_code == 200
        assert stylesheet.status_code == 200
        assert "SwaggerUIBundle" in javascript.text
        assert ".swagger-ui" in stylesheet.text


@pytest.mark.asyncio
async def test_redoc_cdn_page_is_disabled() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test",
    ) as client:
        assert (await client.get("/redoc")).status_code == 404
