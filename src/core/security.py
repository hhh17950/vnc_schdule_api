from hmac import compare_digest

from fastapi import Header, HTTPException

from .config import get_settings


async def require_worker(authorization: str = Header(default="")) -> None:
    expected = f"Bearer {get_settings().internal_token}"
    if not compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="invalid worker token")
