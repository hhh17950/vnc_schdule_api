import httpx
import pytest

from src.main import app


@pytest.mark.asyncio
async def test_creation_contract_without_key() -> None:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        schema = (await client.get("/openapi.json")).json()
        parameters = schema["paths"]["/api/v1/tasks"]["post"].get("parameters", [])
        assert not any(p["name"] == "idempotency-key" for p in parameters)
        payload = {"command": ["/bin/echo", "test"], "cwd": "/tmp"}
        first = await client.post("/api/v1/tasks", json=payload)
        second = await client.post("/api/v1/tasks", json=payload)
        assert first.status_code == second.status_code == 201
        assert first.json()["id"] != second.json()["id"]
        assert first.json()["cwd"] == "/tmp"
        assert (await client.post("/api/v1/tasks", json={**payload, "cwd": "."})).status_code == 422
