from __future__ import annotations

import json

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.worker.main as worker_main
from app.core.config import settings
from app.models.outbox import OutboxEvent
from app.models.task import Task
from app.services import tasks as tasks_service
from app.worker.services import attempts as attempts_service
from app.worker.services import scheduler as retry_scheduler


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
async def test_duplicate_idempotency_key_replays_same_task(client) -> None:
    payload = {"task_type": "echo", "payload": {}, "idempotency_key": "dup-1"}
    first = await client.post("/api/v1/tasks", json=payload)
    second = await client.post("/api/v1/tasks", json=payload)
    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["task"]["id"] == first.json()["task"]["id"]


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
async def test_cancel_queued_and_running(client, session_factory, monkeypatch) -> None:
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
async def test_cancel_running_task_and_worker_skips(client, session_factory, monkeypatch) -> None:
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
async def test_retry_dead_lettered_task_requeues(client, session_factory) -> None:
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 1, "payload": {"reason": "x"}},
    )
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        task = await _task(session, task_id)
        attempt = await attempts_service.start_attempt(session, task)
        attempt.worker_id = "test-worker"
        await attempts_service.record_failure(session, task, attempt, "boom", retryable=True)
        await session.commit()
        assert task.status == "dead_letter"

    retried = await client.post(f"/api/v1/tasks/{task_id}/retry")
    assert retried.status_code == 200
    assert retried.json()["task"]["status"] == "queued"

    conflict = await client.post(f"/api/v1/tasks/{task_id}/retry")
    assert conflict.status_code == 409

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.attempts == 1
        assert task.finished_at is None
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
        assert len(outbox) == 2


@pytest.mark.asyncio
async def test_worker_success_and_result(client, session_factory, monkeypatch) -> None:
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
async def test_worker_fail_handler_schedules_retry(client, session_factory, monkeypatch) -> None:
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "fail", "payload": {"reason": "boom"}}
    )
    task_id = created.json()["task"]["id"]

    monkeypatch.setattr(settings, "retry_base_seconds", 10.0)
    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)
    assert message.acked

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "retry_scheduled"
        assert task.attempts == 1
        assert task.last_error == "Запит на помилку: boom"
        assert task.scheduled_at is not None
        assert task.finished_at is None
        attempt = task.attempts_[-1]
        assert attempt.status == "failed"
        assert attempt.error_message == "Запит на помилку: boom"
        last_event = (await tasks_service.get_events(session, task_id))[-1]
        assert last_event.event_type == "task.retry_scheduled"
        assert last_event.details["backoff_seconds"] == 10.0


@pytest.mark.asyncio
async def test_worker_unknown_type_fails(client, session_factory, monkeypatch) -> None:
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
async def test_worker_timeout_schedules_retry(client, session_factory, monkeypatch) -> None:
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
        assert task.status == "retry_scheduled"
        assert "Таймаут" in (task.last_error or "")


@pytest.mark.asyncio
async def test_worker_exhausted_attempts_moves_to_dead_letter(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 1, "payload": {"reason": "boom"}},
    )
    task_id = created.json()["task"]["id"]

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)
    assert message.acked

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "dead_letter"
        assert task.attempts == 1
        assert task.finished_at is not None
        last_event = (await tasks_service.get_events(session, task_id))[-1]
        assert last_event.event_type == "task.dead_lettered"
        assert last_event.new_status == "dead_letter"


@pytest.mark.asyncio
async def test_retry_scheduler_requeues_due_task(client, session_factory, monkeypatch) -> None:
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 3, "payload": {"reason": "x"}},
    )
    task_id = created.json()["task"]["id"]

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    monkeypatch.setattr(settings, "retry_base_seconds", 0.0)
    await worker_main.handle_message(message)
    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "retry_scheduled"

    monkeypatch.setattr(retry_scheduler, "session_factory", session_factory)
    requeued = await retry_scheduler.requeue_due_tasks()
    assert requeued == 1

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "queued"
        assert task.scheduled_at is None
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
        assert len(outbox) == 2


@pytest.mark.asyncio
async def test_retry_scheduler_skips_future_task(client, session_factory, monkeypatch) -> None:
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 3, "payload": {"reason": "x"}},
    )
    task_id = created.json()["task"]["id"]

    message = _msg_for(task_id)
    _patch_worker(monkeypatch, session_factory)
    await worker_main.handle_message(message)

    from datetime import UTC, datetime, timedelta

    async with session_factory() as session:
        task = await _task(session, task_id)
        task.scheduled_at = datetime.now(UTC) + timedelta(hours=1)
        await session.commit()

    monkeypatch.setattr(retry_scheduler, "session_factory", session_factory)
    requeued = await retry_scheduler.requeue_due_tasks()
    assert requeued == 0

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "retry_scheduled"


@pytest.mark.asyncio
async def test_full_retry_cycle_to_dead_letter(client, session_factory, monkeypatch) -> None:
    from datetime import UTC, datetime, timedelta

    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 3, "payload": {"reason": "x"}},
    )
    task_id = created.json()["task"]["id"]

    _patch_worker(monkeypatch, session_factory)
    monkeypatch.setattr(retry_scheduler, "session_factory", session_factory)

    delays: list[float] = []

    for _ in range(2):
        message = _msg_for(task_id)
        await worker_main.handle_message(message)
        assert message.acked
        async with session_factory() as session:
            task = await _task(session, task_id)
            event = (await tasks_service.get_events(session, task_id))[-1]
            assert event.event_type == "task.retry_scheduled"
            delays.append(event.details["backoff_seconds"])
            task.scheduled_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()
        requeued = await retry_scheduler.requeue_due_tasks()
        assert requeued == 1

    message = _msg_for(task_id)
    await worker_main.handle_message(message)
    assert message.acked

    async with session_factory() as session:
        task = await _task(session, task_id)
        assert task.status == "dead_letter"
        assert task.attempts == 3
        last_event = (await tasks_service.get_events(session, task_id))[-1]
        assert last_event.event_type == "task.dead_lettered"

    assert delays == [1.0, 2.0]
