from __future__ import annotations

import json

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.worker.main as worker_main
from app.models.outbox import OutboxEvent
from app.models.task import Task
from app.services import tasks as tasks_service
from app.worker.services import attempts as attempts_service


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.acked = False
        self.nacked = False

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = False) -> None:
        self.nacked = True


def _msg_for(task_id: int) -> FakeMessage:
    return FakeMessage(json.dumps({"task_id": task_id}).encode())


def _patch_worker(monkeypatch, factory: async_sessionmaker) -> None:
    monkeypatch.setattr(worker_main, "session_factory", factory)


async def _task(session: AsyncSession, task_id: int) -> Task:
    task = await tasks_service.get_task(session, task_id)
    assert task is not None
    return task


@pytest.mark.asyncio
async def test_create_task_queued_with_events(client) -> None:
    response = await client.post(
        "/api/v1/tasks",
        json={"task_type": "echo", "payload": {"message": "hi"}},
    )
    assert response.status_code == 201
    body = response.json()["task"]
    assert body["status"] == "queued"
    assert body["task_type"] == "echo"
    assert body["priority"] == "normal"
    assert body["max_attempts"] == 3

    events_response = await client.get(f"/api/v1/tasks/{body['id']}/events")
    events = events_response.json()["events"]
    assert [e["event_type"] for e in events] == ["task.created", "task.queued"]


@pytest.mark.asyncio
async def test_create_task_writes_outbox_row(client, session_factory) -> None:
    response = await client.post(
        "/api/v1/tasks",
        json={"task_type": "sleep", "payload": {"seconds": 1}},
    )
    task_id = response.json()["task"]["id"]
    async with session_factory() as session:
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
    assert len(outbox) == 1
    assert outbox[0].status == "pending"
    assert outbox[0].routing_key == "task_created"
    assert outbox[0].payload["task_id"] == task_id


@pytest.mark.asyncio
async def test_duplicate_idempotency_key_conflict(client) -> None:
    payload = {"task_type": "echo", "payload": {}, "idempotency_key": "dup-1"}
    first = await client.post("/api/v1/tasks", json=payload)
    second = await client.post("/api/v1/tasks", json=payload)
    assert first.status_code == 201
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_list_tasks_with_filter(client) -> None:
    await client.post("/api/v1/tasks", json={"task_type": "echo"})
    await client.post("/api/v1/tasks", json={"task_type": "fail"})
    response = await client.get("/api/v1/tasks?task_type=echo")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["task_type"] == "echo"


@pytest.mark.asyncio
async def test_cancel_queued_and_running(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    cancelled = await client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["task"]["status"] == "cancelled"
    cancelled_again = await client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert cancelled_again.status_code == 409

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)
    assert message.acked
    assert message.nacked is False
    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "cancelled"
        assert task.attempts == 0


@pytest.mark.asyncio
async def test_cancel_running_task_and_worker_skips(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        task = await _task(session, task_id)
        attempt = await attempts_service.start_attempt(session, task)
        attempt.worker_id = "test-worker"
        await session.commit()
        assert task.status == "running"

    gone = await client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert gone.status_code == 200
    assert gone.json()["task"]["status"] == "cancelled"

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)
    assert message.acked
    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "cancelled"
        assert task.attempts == 1


@pytest.mark.asyncio
async def test_retry_failed_task_requeues(client, session_factory) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "fail"})
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        task = await _task(session, task_id)
        attempt = await attempts_service.start_attempt(session, task)
        attempt.worker_id = "test-worker"
        await attempts_service.record_failure(session, task, attempt, "boom")
        await session.commit()
        assert task.status == "failed"

    retried = await client.post(f"/api/v1/tasks/{task_id}/retry")
    assert retried.status_code == 200
    assert retried.json()["task"]["status"] == "queued"

    conflict = await client.post(f"/api/v1/tasks/{task_id}/retry")
    assert conflict.status_code == 409

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.attempts == 1
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
        assert len(outbox) == 2


@pytest.mark.asyncio
async def test_worker_success_and_result(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "echo", "payload": {"message": "hi"}}
    )
    task_id = created.json()["task"]["id"]

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)
    assert message.acked

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "success"
        assert task.attempts == 1
        assert task.started_at is not None
        assert task.finished_at is not None
        assert task.result == {"message": "hi", "task_type": "echo"}
        assert (await tasks_service.get_events(session, task_id))[-1].event_type == "task.succeeded"


@pytest.mark.asyncio
async def test_worker_fail_handler_records_error(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "fail", "payload": {"reason": "boom"}}
    )
    task_id = created.json()["task"]["id"]

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)
    assert message.acked

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "failed"
        assert task.last_error == "Запит на помилку: boom"
        assert task.attempts == 1
        attempt = task.attempts_[-1]
        assert attempt.status == "failed"
        assert attempt.error_message == "Запит на помилку: boom"


@pytest.mark.asyncio
async def test_worker_unknown_type_fails(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "nonsense"})
    task_id = created.json()["task"]["id"]

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)
    assert message.acked

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "failed"
        assert "Невідомий тип задачі" in (task.last_error or "")


@pytest.mark.asyncio
async def test_worker_timeout_marks_failed(
    client, session_factory, monkeypatch
) -> None:
    from app.core.config import settings

    monkeypatch.setattr(settings, "task_execution_timeout_seconds", 0.2)
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "sleep", "payload": {"seconds": 30}}
    )
    task_id = created.json()["task"]["id"]

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)
    assert message.acked

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "failed"
        assert "Таймаут" in (task.last_error or "")
