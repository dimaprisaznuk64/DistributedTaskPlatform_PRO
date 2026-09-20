from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

import app.worker.main as worker_main
import app.worker.services.scheduler as scheduler
from app.models.outbox import OutboxEvent
from app.models.task import Task
from app.services import tasks as tasks_service


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


async def _drain(session_factory, task_ids: list[int], monkeypatch) -> None:
    monkeypatch.setattr(worker_main, "session_factory", session_factory)
    for task_id in task_ids:
        message = _msg_for(task_id)
        await worker_main.handle_message(message)
        assert message.acked


async def _queued_ids(session_factory) -> list[int]:
    async with session_factory() as session:
        rows = await session.execute(
            select(Task.id).where(Task.status == "queued").order_by(Task.id.asc())
        )
        return [row[0] for row in rows.all()]


async def _make_retries_due(session_factory) -> None:
    async with session_factory() as session:
        for task in (
            await session.execute(
                select(Task).where(Task.status == "retry_scheduled")
            )
        ).scalars().all():
            task.scheduled_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()


@pytest.mark.asyncio
async def test_burst_tasks_all_drain_success(client, session_factory, monkeypatch) -> None:
    task_ids = []
    for _ in range(25):
        created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
        task_ids.append(created.json()["task"]["id"])

    await _drain(session_factory, task_ids, monkeypatch)

    async with session_factory() as session:
        for task_id in task_ids:
            task = await tasks_service.get_task(session, task_id)
            assert task.status == "success"
            assert task.attempts == 1
            assert task.result["task_type"] == "echo"
            assert task.lease_owner is None
        outbox_rows = (
            await session.execute(select(OutboxEvent).order_by(OutboxEvent.id))
        ).all()
        assert len(outbox_rows) == len(task_ids)


@pytest.mark.asyncio
async def test_duplicate_message_processed_once(client, session_factory, monkeypatch) -> None:
    created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = created.json()["task"]["id"]

    monkeypatch.setattr(worker_main, "session_factory", session_factory)
    first = _msg_for(task_id)
    await worker_main.handle_message(first)
    second = _msg_for(task_id)
    await worker_main.handle_message(second)
    assert first.acked and second.acked

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "success"
        assert task.attempts == 1


@pytest.mark.asyncio
async def test_burst_failures_exhaust_to_dead_letter(
    client, session_factory, monkeypatch
) -> None:
    task_ids = []
    for _ in range(5):
        created = await client.post(
            "/api/v1/tasks",
            json={"task_type": "fail", "max_attempts": 3, "payload": {"reason": "load"}},
        )
        task_ids.append(created.json()["task"]["id"])

    monkeypatch.setattr(worker_main, "session_factory", session_factory)
    monkeypatch.setattr(scheduler, "session_factory", session_factory)

    for _round in range(8):
        await _make_retries_due(session_factory)
        await scheduler.requeue_due_tasks()
        for task_id in await _queued_ids(session_factory):
            message = _msg_for(task_id)
            await worker_main.handle_message(message)
            assert message.acked

    async with session_factory() as session:
        for task_id in task_ids:
            task = await tasks_service.get_task(session, task_id)
            assert task.status == "dead_letter", (
                f"task {task_id}: {task.status} after {task.attempts} attempts"
            )
            assert task.attempts == 3
            events = await tasks_service.get_events(session, task_id)
            assert events[-1].event_type == "task.dead_lettered"


@pytest.mark.asyncio
async def test_metrics_endpoint_reports_state(client, session_factory, monkeypatch) -> None:
    import app.api.routes.metrics as metrics_route

    await client.post("/api/v1/tasks", json={"task_type": "echo"})
    monkeypatch.setattr(metrics_route, "session_factory", session_factory)

    response = await client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    text = response.text
    assert "tasks_created_total" in text
    assert 'tasks_status{status="queued"} 1.0' in text
    assert "workers_active 0.0" in text
    assert "outbox_pending" in text
    assert "http_requests_total" in text or "http_request_duration_seconds" in text


async def _dlq_ids(session_factory) -> list[int]:
    async with session_factory() as session:
        rows = await session.execute(
            select(Task.id).where(Task.status == "dead_letter").order_by(Task.id.asc())
        )
        return [row[0] for row in rows.all()]


async def _make_dlq_retries_due(session_factory) -> None:
    async with session_factory() as session:
        for task in (
            await session.execute(select(Task).where(Task.status == "dead_letter"))
        ).scalars().all():
            task.dlq_retry_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()


@pytest.mark.asyncio
async def test_dead_letter_task_requeued_by_scheduler_until_success(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 2, "payload": {"reason": "dlq"}},
    )
    task_id = created.json()["task"]["id"]

    monkeypatch.setattr(worker_main, "session_factory", session_factory)
    monkeypatch.setattr(scheduler, "session_factory", session_factory)
    monkeypatch.setattr(scheduler.settings, "dlq_retry_interval_seconds", 600.0)

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.max_attempts == 2

    # Немає ініціальної постановки в DRQ до вичерпання спроб
    assert await _dlq_ids(session_factory) == []

    # Проганяємо спроби, поки задача не опиниться в DLQ
    for _ in range(4):
        await _make_retries_due(session_factory)
        await scheduler.requeue_due_tasks()
        for qid in await _queued_ids(session_factory):
            msg = _msg_for(qid)
            await worker_main.handle_message(msg)
            assert msg.acked

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "dead_letter"
        assert task.dlq_requeue_count == 0

    # До настання dlq_retry_at планувальник не чіпає
    assert await scheduler.requeue_dlq_tasks() == 0

    # Після настання — повертає через DRQ-плин
    await _make_dlq_retries_due(session_factory)
    requeued = await scheduler.requeue_dlq_tasks()
    assert requeued == 1
    assert await _queued_ids(session_factory) != []

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "queued"
        assert task.dlq_requeue_count == 1
        events = await tasks_service.get_events(session, task_id)
        assert events[-1].event_type == "task.dlq_requeued"


@pytest.mark.asyncio
async def test_dlq_requeues_stop_after_max_cycles(
    client, session_factory, monkeypatch
) -> None:
    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 1, "payload": {"reason": "dlq-max"}},
    )
    task_id = created.json()["task"]["id"]

    monkeypatch.setattr(worker_main, "session_factory", session_factory)
    monkeypatch.setattr(scheduler, "session_factory", session_factory)
    monkeypatch.setattr(scheduler.settings, "dlq_retry_max_cycles", 2)
    monkeypatch.setattr(scheduler.settings, "dlq_retry_interval_seconds", 0.01)

    # Проганяємо спробу → потрапляє в DLQ
    for qid in await _queued_ids(session_factory):
        msg = _msg_for(qid)
        await worker_main.handle_message(msg)
        assert msg.acked

    cycles = 0
    for _ in range(6):
        await _make_dlq_retries_due(session_factory)
        requeued = await scheduler.requeue_dlq_tasks()
        for qid in await _queued_ids(session_factory):
            msg = _msg_for(qid)
            await worker_main.handle_message(msg)
            assert msg.acked
            cycles += requeued

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "dead_letter"
        assert task.dlq_requeue_count == 2
    # Подальші DLQ-цикли не чіпають задачу (ліміт циклів вичерпано)
    await _make_dlq_retries_due(session_factory)
    assert await scheduler.requeue_dlq_tasks() == 0

    async with session_factory() as session:
        task = await tasks_service.get_task(session, task_id)
        assert task.status == "dead_letter"
        assert task.dlq_requeue_count == 2
