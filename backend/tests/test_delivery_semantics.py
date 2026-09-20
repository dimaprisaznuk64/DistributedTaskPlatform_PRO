from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

import app.services.outbox as outbox_service
import app.worker.main as worker_main
from app.models.outbox import OUTBOX_PENDING, OUTBOX_SENT, OutboxEvent
from app.models.task import Task
from app.services import tasks as tasks_service
from app.worker.services import attempts as attempts_service
from app.worker.services import scheduler as coordinator


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


class FakeExchange:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[tuple[bytes, str]] = []
        self.fail = fail

    async def publish(self, message, routing_key: str | None = None) -> None:
        if self.fail:
            raise RuntimeError("broker unavailable")
        self.published.append((message.body, routing_key))


class FakeChannel:
    def __init__(self, fail: bool = False) -> None:
        self.exchange = FakeExchange(fail=fail)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args) -> None:
        return None

    async def declare_exchange(self, *args, **kwargs):
        return self.exchange


class FakeConnection:
    def __init__(self, fail: bool = False) -> None:
        self._channel = FakeChannel(fail=fail)

    def channel(self):
        return self._channel


async def _get_outbox(session_factory) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list((await session.execute(select(OutboxEvent))).scalars().all())


async def _task_state(session_factory, task_id: int) -> Task:
    async with session_factory() as session:
        return await tasks_service.get_task(session, task_id)


def _patch_connection(monkeypatch, connection: FakeConnection) -> None:
    async def _connect() -> FakeConnection:
        return connection

    monkeypatch.setattr(outbox_service, "get_connection", _connect)


async def _book_as_running(
    session_factory, task_id: int, worker_id: str, *, expire_lease: bool
) -> None:
    """Симулює запуск задачі воркером без повного виконання (як у реальному worker)."""
    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        await attempts_service.try_acquire_lease(
            session, task_id, worker_id, lease_seconds=60
        )
        attempt = await attempts_service.start_attempt(session, task)
        attempt.worker_id = worker_id
        if expire_lease:
            task.lease_expires_at = datetime.now(UTC) - timedelta(seconds=5)
        await session.commit()


@pytest.mark.asyncio
async def test_outbox_publish_marks_sent_after_success(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    monkeypatch.setattr(outbox_service, "session_factory", session_factory)
    connection = FakeConnection()
    _patch_connection(monkeypatch, connection)

    [outbox] = await _get_outbox(session_factory)
    assert outbox.status == OUTBOX_PENDING

    published = await outbox_service.publish_pending_events()
    assert published == 1
    assert connection._channel.exchange.published[0][1] == "task_created"
    assert json.loads(connection._channel.exchange.published[0][0])["task_id"] == task_id

    async with session_factory() as session:
        outbox = await session.get(OutboxEvent, outbox.id)
    assert outbox.status == OUTBOX_SENT
    assert outbox.processed_at is not None
    assert outbox.last_error is None


@pytest.mark.asyncio
async def test_outbox_failure_keeps_pending_then_retries_success(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    monkeypatch.setattr(outbox_service, "session_factory", session_factory)
    _patch_connection(monkeypatch, FakeConnection(fail=True))
    await outbox_service.publish_pending_events()

    async with session_factory() as session:
        failed = (
            await session.execute(select(OutboxEvent).order_by(OutboxEvent.id))
        ).scalar_one()
        assert failed.status == OUTBOX_PENDING
        assert failed.attempts >= 1
        assert "broker unavailable" in failed.last_error

    good = FakeConnection()
    _patch_connection(monkeypatch, good)
    published = await outbox_service.publish_pending_events()
    assert published == 1

    async with session_factory() as session:
        ok = await session.get(OutboxEvent, failed.id)
    assert ok.status == OUTBOX_SENT
    assert ok.last_error is None
    assert len(good._channel.exchange.published) == 1
    assert json.loads(good._channel.exchange.published[0][0])["task_id"] == task_id


@pytest.mark.asyncio
async def test_exactly_once_booking_duplicate_message_while_running(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "echo", "payload": {"message": "once"}}
    )
    task_id = created.json()["task"]["id"]

    monkeypatch.setattr(worker_main, "session_factory", session_factory)

    await _book_as_running(
        session_factory, task_id, worker_main.WORKER_ID, expire_lease=False
    )

    duplicate = _msg_for(task_id)
    await worker_main.handle_message(duplicate)
    assert duplicate.acked
    assert duplicate.nacked is False

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "running"
        assert task.attempts == 1
        assert len(task.attempts_) == 1
        running_events = [
            e for e in await tasks_service.get_events(session, task_id)
            if e.event_type == "task.running"
        ]
    assert len(running_events) == 1


@pytest.mark.asyncio
async def test_at_least_once_redelivery_after_worker_death_runs_again(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "echo", "payload": {"message": "lost-ack"}}
    )
    task_id = created.json()["task"]["id"]

    monkeypatch.setattr(worker_main, "session_factory", session_factory)
    monkeypatch.setattr(coordinator, "session_factory", session_factory)

    await _book_as_running(
        session_factory, task_id, worker_main.WORKER_ID, expire_lease=True
    )

    recovered = await coordinator.recover_stuck_tasks()
    assert recovered == 1

    redelivered = _msg_for(task_id)
    await worker_main.handle_message(redelivered)
    assert redelivered.acked

    task = await _task_state(session_factory, task_id)
    assert task.status == "success"
    assert task.attempts == 2

    async with session_factory() as session:
        fresh = await tasks_service.get_task(session, task_id)
        assert fresh.attempts_[-1].status == "success"
