from __future__ import annotations

import contextlib
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services import events as events_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["ws"])


@router.websocket("/events")
async def ws_events(websocket: WebSocket) -> None:
    """Live-стрічка подій: проксіює Redis pub/sub на дашборд."""
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
