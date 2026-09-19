from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

import app.worker.main as worker_main
from app.models.outbox import OutboxEvent
from app.models.task import Task
from app.services import tasks as tasks_service
from app.services import workers as workers_service
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


def _now() -> datetime:
    return datetime.now(UTC)


@pytest.mark.asyncio
async def test_create_in_future_scheduled_status_no_outbox(client, session_factory) -> None:
    scheduled = _now() + timedelta(hours=1)
    response = await client.post(
        "/api/v1/tasks",
        json={"task_type": "echo", "schedule_at": scheduled.isoformat()},
    )
    assert response.status_code == 201
    body = response.json()["task"]
    assert body["status"] == "scheduled"

    async with session_factory() as session:
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
        assert outbox == []
        events = await tasks_service.get_events(session, body["id"])
        assert [e.event_type for e in events] == ["task.created", "task.scheduled"]


@pytest.mark.asyncio
async def test_schedule_at_in_past_queued_immediately(client, session_factory) -> None:
    scheduled = _now() - timedelta(minutes=5)
    response = await client.post(
        "/api/v1/tasks",
        json={"task_type": "echo", "schedule_at": scheduled.isoformat()},
    )
    assert response.status_code == 201
    assert response.json()["task"]["status"] == "queued"


@pytest.mark.asyncio
async def test_cancel_scheduled_task(client, session_factory) -> None:
    scheduled = _now() + timedelta(hours=1)
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "echo", "schedule_at": scheduled.isoformat()},
    )
    task_id = created.json()["task"]["id"]

    cancelled = await client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["task"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_scheduler_requeues_due_scheduled_task(client, session_factory, monkeypatch) -> None:
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "echo", "schedule_at": (_now() + timedelta(hours=1)).isoformat()},
    )
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        task.scheduled_at = _now() - timedelta(seconds=1)
        await session.commit()

    monkeypatch.setattr(coordinator, "session_factory", session_factory)
    requeued = await coordinator.requeue_due_tasks()
    assert requeued == 1

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "queued"
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
        assert len(outbox) == 1
        events = await tasks_service.get_events(session, task_id)
        assert events[-1].event_type == "task.queued"


@pytest.mark.asyncio
async def test_worker_register_and_list_endpoint(client, session_factory) -> None:
    async with session_factory() as session:
        await workers_service.register_worker(session, "w-1", hostname="host-a", pid=11)
        await session.commit()

    response = await client.get("/api/v1/workers")
    assert response.status_code == 200
    items = response.json()
    assert items[0]["worker_id"] == "w-1"
    assert items[0]["hostname"] == "host-a"
    assert items[0]["status"] == "alive"


@pytest.mark.asyncio
async def test_worker_heartbeat_updates_timestamp(session_factory) -> None:
    async with session_factory() as session:
        await workers_service.register_worker(session, "w-2", hostname="host-b", pid=22)
        await session.commit()

    async with session_factory() as session:
        worker = await workers_service.heartbeat(session, "w-2")
        assert worker is not None
        assert worker.last_heartbeat_at <= _now()


@pytest.mark.asyncio
async def test_lease_acquired_only_by_one_worker(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        first = await attempts_service.try_acquire_lease(
            session, task_id, "worker-a", lease_seconds=60
        )
        second = await attempts_service.try_acquire_lease(
            session, task_id, "worker-b", lease_seconds=60
        )
        await session.commit()

    assert first is True
    assert second is False

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.lease_owner == "worker-a"
        assert task.lease_expires_at is not None


@pytest.mark.asyncio
async def test_lease_claimable_after_expiry(client, session_factory) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        assert await attempts_service.try_acquire_lease(
            session, task_id, "worker-a", lease_seconds=60
        )
        await session.commit()

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        task.lease_expires_at = _now() - timedelta(seconds=5)
        await session.commit()

    async with session_factory() as session:
        reclaimed = await attempts_service.try_acquire_lease(
            session, task_id, "worker-b", lease_seconds=60
        )
        await session.commit()

    assert reclaimed is True
    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.lease_owner == "worker-b"


@pytest.mark.asyncio
async def test_renew_lease_only_for_owner(client, session_factory) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        await attempts_service.try_acquire_lease(session, task_id, "worker-a", 60)
        task = await tasks_service.get_task(session, task_id)
        await attempts_service.start_attempt(session, task)
        await session.commit()

    async with session_factory() as session:
        ok_owner = await attempts_service.renew_lease(
            session, task_id, "worker-a", 60
        )
        ok_other = await attempts_service.renew_lease(
            session, task_id, "worker-b", 60
        )
        await session.commit()

    assert ok_owner is True
    assert ok_other is False


def _expire_lease(task: Task) -> None:
    task.lease_expires_at = _now() - timedelta(seconds=5)


@pytest.mark.asyncio
async def test_recover_running_task_with_expired_lease(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "sleep", "payload": {"seconds": 30}}
    )
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        attempt = await attempts_service.start_attempt(session, task)
        attempt.worker_id = "w-dead"
        task.lease_owner = "w-dead"
        _expire_lease(task)
        await session.commit()

    monkeypatch.setattr(coordinator, "session_factory", session_factory)
    recovered = await coordinator.recover_stuck_tasks()
    assert recovered == 1

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "queued"
        assert task.attempts == 1
        assert task.lease_owner is None
        assert task.attempts_[-1].status == "failed"
        assert (
            task.attempts_[-1].error_message
            == "Lease прострочився — задачу повернуто в чергу"
        )
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
        assert len(outbox) == 2
        events = await tasks_service.get_events(session, task_id)
        assert events[-1].event_type == "task.requeued"


@pytest.mark.asyncio
async def test_recover_expired_lease_still_queued_republishes(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        task.lease_owner = "w-gone"
        _expire_lease(task)
        await session.commit()

    monkeypatch.setattr(coordinator, "session_factory", session_factory)
    recovered = await coordinator.recover_stuck_tasks()
    assert recovered == 1

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "queued"
        assert task.lease_owner is None
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
        assert len(outbox) == 2


@pytest.mark.asyncio
async def test_worker_claim_then_requeued_runs_again(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "echo", "payload": {"message": "back"}}
    )
    task_id = created.json()["task"]["id"]

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        attempt = await attempts_service.start_attempt(session, task)
        attempt.worker_id = "w-dead-2"
        task.lease_owner = "w-dead-2"
        _expire_lease(task)
        await session.commit()

    monkeypatch.setattr(coordinator, "session_factory", session_factory)
    assert await coordinator.recover_stuck_tasks() == 1

    message = _msg_for(task_id)
    monkeypatch.setattr(worker_main, "session_factory", session_factory)
    await worker_main.handle_message(message)
    assert message.acked

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "success"
        assert task.attempts == 2
        assert task.lease_owner is None
