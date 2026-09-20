from __future__ import annotations

import contextlib
import logging

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.services import auth as auth_service
from app.services import events as events_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["ws"])


async def _authenticate_ws(websocket: WebSocket, token: str) -> int | None:
    """Повертає user_id або None при провалі аутентифікації."""
    try:
        payload = auth_service.decode_token(token)
        if payload.get("type") != auth_service.TOKEN_TYPE_ACCESS:
            return None
        from app.db.session import session_factory

        async with session_factory() as session:
            user = await auth_service.get_user_by_id(session, int(payload["sub"]))
            if user is None or not user.is_active:
                return None
            return user.id
    except Exception:
        return None


@router.websocket("/events")
async def ws_events(
    websocket: WebSocket,
    token: str = Query(default=""),
) -> None:
    """Live-стрічка подій: проксіює Redis pub/sub на дашборд (access-токен у query)."""
    user_id = await _authenticate_ws(websocket, token)
    if user_id is None:
        await websocket.close(code=4401)
        return

    from app.core.rate_limit import ws_allowed

    if not await ws_allowed(str(user_id)):
        await websocket.close(code=4429)
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
