from __future__ import annotations

import contextlib
import logging

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.services import auth as auth_service
from app.services import events as events_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["ws"])


async def _authenticate_ws(websocket: WebSocket, token: str) -> bool:
    try:
        payload = auth_service.decode_token(token)
        if payload.get("type") != auth_service.TOKEN_TYPE_ACCESS:
            return False
        from app.db.session import session_factory

        async with session_factory() as session:
            user = await auth_service.get_user_by_id(session, int(payload["sub"]))
            return user is not None and user.is_active
    except Exception:
        return False


@router.websocket("/events")
async def ws_events(
    websocket: WebSocket,
    token: str = Query(default=""),
) -> None:
    """Live-стрічка подій: проксіює Redis pub/sub на дашборд (access-токен у query)."""
    if not await _authenticate_ws(websocket, token):
        await websocket.close(code=4401)
        return
    await websocket.accept()
    try:
        async for frame in events_service.event_stream():
            await websocket.send_text(frame)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("WS-потік подій перервано")
    finally:
        with contextlib.suppress(Exception):
            await websocket.close()
