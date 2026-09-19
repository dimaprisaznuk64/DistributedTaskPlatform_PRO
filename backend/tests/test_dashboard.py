from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

import app.api.routes.ws as ws_route
import app.worker.services.scheduler as scheduler
from app.main import app
from app.models.task import Task
from app.models.worker import Worker
from app.services import tasks as tasks_service
from app.services import workers as workers_service


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def schedule_task_event(self, event_type: str, task) -> None:
        self.calls.append((event_type, {"task_id": task.id, "status": task.status}))

    def schedule_worker_event(self, event_type: str, worker_id: str) -> None:
        self.calls.append((event_type, {"worker_id": worker_id}))


@pytest.mark.asyncio
async def test_stats_endpoint(client, session_factory) -> None:
    await client.post("/api/v1/tasks", json={"task_type": "echo"})
    await client.post(
        "/api/v1/tasks",
        json={"task_type": "http.request", "priority": "high"},
    )
    async with session_factory() as session:
        await workers_service.register_worker(session, "w-1", hostname="h1", pid=1)
        await workers_service.register_worker(session, "w-2", hostname="h2", pid=2)
        worker = await workers_service.heartbeat(session, "w-2")
        worker.status = "dead"
        task = (
            (await session.execute(select(Task).order_by(Task.id.asc())))
            .scalars()
            .first()
        )
        task.status = "success"
        await session.commit()

    response = await client.get("/api/v1/stats")
    assert response.status_code == 200
    body = response.json()
    assert body["tasks"]["total"] == 2
    assert body["tasks"]["by_status"]["success"] == 1
    assert body["tasks"]["by_status"]["queued"] == 1
    assert body["tasks"]["by_priority"]["high"] == 1
    assert body["tasks"]["by_type"]["echo"] == 1
    assert body["workers"]["total"] == 2
    assert body["workers"]["by_status"]["alive"] == 1
    assert body["workers"]["by_status"]["dead"] == 1
    assert body["queue"]["depth"] is None or isinstance(body["queue"]["depth"], int)


@pytest.mark.asyncio
async def test_create_publishes_redis_event(client, monkeypatch) -> None:
    fake = FakePublisher()
    monkeypatch.setattr("app.api.routes.tasks.events_service", fake)
    response = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    assert response.status_code == 201
    assert [e for e in fake.calls if e[0] == "task.updated"]


@pytest.mark.asyncio
async def test_scheduler_publishes_requeue_event(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "echo", "schedule_at": "2099-01-01T00:00:00+00:00"},
    )
    task_id = created.json()["task"]["id"]
    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        task.scheduled_at = datetime.now(UTC) - timedelta(minutes=1)
        await session.commit()

    fake = FakePublisher()
    monkeypatch.setattr(scheduler, "events_service", fake)
    monkeypatch.setattr(scheduler, "session_factory", session_factory)
    assert await scheduler.requeue_due_tasks() == 1
    assert fake.calls[0] == ("task.updated", {"task_id": task_id, "status": "queued"})


@pytest.mark.asyncio
async def test_recover_publishes_dead_worker_event(
    client, session_factory, monkeypatch
) -> None:
    async with session_factory() as session:
        await workers_service.register_worker(session, "w-old", hostname="h", pid=1)
        worker = (await session.execute(select(Worker))).scalars().first()
        worker.last_heartbeat_at = datetime.now(UTC) - timedelta(hours=2)
        await session.commit()

    fake = FakePublisher()
    monkeypatch.setattr(scheduler, "events_service", fake)
    monkeypatch.setattr(scheduler, "session_factory", session_factory)
    assert await scheduler.recover_stuck_tasks() == 0
    assert ("worker.dead", {"worker_id": "w-old"}) in fake.calls


@pytest.mark.asyncio
async def test_ws_stream_forwards_frames(client, monkeypatch) -> None:
    frames = [
        json.dumps({"type": "task.updated", "payload": {"task_id": 1, "status": "queued"}}),
        json.dumps({"type": "worker.alive", "payload": {"worker_id": "w-1"}}),
    ]

    async def fake_stream():
        for frame in frames:
            yield frame

    monkeypatch.setattr(ws_route.events_service, "event_stream", fake_stream)

    from starlette.testclient import TestClient

    from app.services import auth as auth_service

    token = auth_service.create_access_token(1)
    with TestClient(app) as tc, tc.websocket_connect(
        f"/api/v1/ws/events?token={token}"
    ) as ws:
        assert ws.receive_text() == frames[0]
        assert ws.receive_text() == frames[1]
