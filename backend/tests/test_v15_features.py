from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

import app.worker.main as worker_main
from app.core.config import settings
from app.main import app
from app.models.audit import AuditLog
from app.models.outbox import OutboxEvent
from app.models.task import Task
from app.models.webhook import WebhookDelivery
from app.services import webhooks as webhooks_service
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


def _patch_scheduler(monkeypatch, factory: async_sessionmaker) -> None:
    monkeypatch.setattr(retry_scheduler, "session_factory", factory)


def _patch_webhooks(monkeypatch, factory: async_sessionmaker) -> None:
    monkeypatch.setattr(webhooks_service, "session_factory", factory)


async def _run(monkeypatch, factory: async_sessionmaker, task_id: int) -> FakeMessage:
    message = _msg_for(task_id)
    _patch_worker(monkeypatch, factory)
    await worker_main.handle_message(message)
    assert message.acked
    return message


class RecordingHandler(BaseHTTPRequestHandler):
    records: ClassVar[list[dict[str, Any]]] = []

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        body = json.loads(raw.decode("utf-8") or b"{}")
        self.records.append(
            {
                "path": self.path,
                "body": body,
                "event": self.headers.get("X-Platform-Event", ""),
                "signature": self.headers.get("X-Platform-Signature", ""),
            }
        )
        response = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def hook_server():
    RecordingHandler.records = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), RecordingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}/hook"
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


async def _new_client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# --- фільтри / пагінація / пошук ---


@pytest.mark.asyncio
async def test_list_pagination_and_filters(client) -> None:
    for i in range(25):
        await client.post(
            "/api/v1/tasks",
            json={
                "task_type": "echo",
                "payload": {"i": i},
                "idempotency_key": f"part-{i:02d}",
            },
        )
    await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "idempotency_key": "bad-key"},
    )

    page1 = await client.get("/api/v1/tasks?limit=10&offset=0")
    assert page1.status_code == 200
    assert page1.json()["total"] == 26
    assert len(page1.json()["items"]) == 10

    page2 = await client.get("/api/v1/tasks?limit=10&offset=20")
    assert len(page2.json()["items"]) == 6

    by_type = await client.get("/api/v1/tasks?task_type=fail")
    assert by_type.json()["total"] == 1

    by_q = await client.get("/api/v1/tasks?q=bad-key")
    assert by_q.json()["total"] == 1
    assert by_q.json()["items"][0]["idempotency_key"] == "bad-key"

    now = datetime.now(UTC)
    window = await client.get(
        "/api/v1/tasks",
        params={
            "created_from": (now - timedelta(minutes=1)).isoformat(),
            "created_to": (now + timedelta(minutes=1)).isoformat(),
        },
    )
    assert window.status_code == 200, window.text
    assert window.json()["total"] == 26

    limited = await client.get("/api/v1/tasks?limit=200")
    assert len(limited.json()["items"]) == 26


# --- bulk-операції ---


@pytest.mark.asyncio
async def test_bulk_create_keeps_going_on_conflict(client) -> None:
    response = await client.post(
        "/api/v1/tasks/bulk",
        json={
            "tasks": [
                {"task_type": "echo", "idempotency_key": "dup"},
                {"task_type": "echo", "idempotency_key": "dup"},
                {"task_type": "fail"},
            ]
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["total"] == 3
    assert len(body["succeeded"]) == 2
    assert len(body["conflicts"]) == 1
    assert body["conflicts"][0]["task_id"] == 0


@pytest.mark.asyncio
async def test_bulk_retry_partial_conflicts(client, session_factory, monkeypatch) -> None:
    dead = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 1, "payload": {"reason": "x"}},
    )
    dead_id = dead.json()["task"]["id"]
    await _run(monkeypatch, session_factory, dead_id)

    queued = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    queued_id = queued.json()["task"]["id"]

    response = await client.post(
        "/api/v1/tasks/bulk/retry", json={"task_ids": [dead_id, queued_id, 999_999]}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert len(body["succeeded"]) == 1
    assert body["succeeded"][0]["id"] == dead_id
    assert len(body["conflicts"]) == 2
    reasons = " ".join(c["reason"] for c in body["conflicts"])
    assert "task not found" in reasons
    assert "Повтор дозволений лише для" in reasons


@pytest.mark.asyncio
async def test_bulk_cancel_partial_conflicts(client) -> None:
    ids = []
    for _ in range(3):
        created = await client.post("/api/v1/tasks", json={"task_type": "echo"})
        ids.append(created.json()["task"]["id"])

    response = await client.post(
        "/api/v1/tasks/bulk/cancel", json={"task_ids": [ids[0], ids[1], 999_999]}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert {t["id"] for t in body["succeeded"]} == {ids[0], ids[1]}
    assert len(body["conflicts"]) == 1
    assert body["conflicts"][0]["task_id"] == 999_999


# --- батчі ---


@pytest.mark.asyncio
async def test_batch_create_progress_cancel(client, session_factory, monkeypatch) -> None:
    response = await client.post(
        "/api/v1/batches",
        json={
            "name": "b1",
            "tasks": [
                {"task_type": "echo", "payload": {"n": 1}},
                {"task_type": "echo", "payload": {"n": 2}},
                {"task_type": "echo", "payload": {"n": 3}},
            ],
        },
    )
    assert response.status_code == 201
    created = response.json()["created"]
    batch_id = response.json()["batch"]["id"]
    assert response.json()["batch"]["progress"]["total"] == 3
    assert response.json()["batch"]["progress"]["queued"] == 3

    detail = await client.get(f"/api/v1/batches/{batch_id}")
    assert detail.status_code == 200
    assert detail.json()["total"] == 3
    assert {t["batch_id"] for t in detail.json()["items"]} == {batch_id}

    await _run(monkeypatch, session_factory, created[0]["id"])
    detail = await client.get(f"/api/v1/batches/{batch_id}")
    progress = detail.json()["batch"]["progress"]
    assert progress["succeeded"] == 1
    assert progress["queued"] == 2
    assert progress["completed"] == 1

    cancelled = await client.post(f"/api/v1/batches/{batch_id}/cancel")
    assert cancelled.status_code == 200
    body = cancelled.json()
    assert body["batch"]["progress"]["cancelled"] == 2
    assert body["batch"]["progress"]["completed"] == 3
    assert len(body["actions"]) == 1
    assert "Скасувати" in body["actions"][0]["reason"]


@pytest.mark.asyncio
async def test_batch_limit_exceeded(client, monkeypatch) -> None:
    monkeypatch.setattr(settings, "batch_max_tasks", 2)
    response = await client.post(
        "/api/v1/batches",
        json={
            "name": "big",
            "tasks": [
                {"task_type": "echo"},
                {"task_type": "echo"},
                {"task_type": "echo"},
            ],
        },
    )
    assert response.status_code == 400
    assert "Забагато задач" in response.json()["detail"]


# --- DAG ---


@pytest.mark.asyncio
async def test_dag_missing_parent_400(client) -> None:
    parent = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    parent_id = parent.json()["task"]["id"]
    child = await client.post(
        "/api/v1/tasks",
        json={"task_type": "echo", "depends_on": [parent_id, 999_999]},
    )
    assert child.status_code == 409
    assert "не знайдено" in child.json()["detail"]


@pytest.mark.asyncio
async def test_dag_success_unblocks_child(client, session_factory, monkeypatch) -> None:
    parent = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    parent_id = parent.json()["task"]["id"]
    child = await client.post(
        "/api/v1/tasks", json={"task_type": "echo", "depends_on": [parent_id]}
    )
    assert child.status_code == 201
    child_id = child.json()["task"]["id"]
    assert child.json()["task"]["status"] == "created"

    events = (await client.get(f"/api/v1/tasks/{child_id}/events")).json()["events"]
    assert [e["event_type"] for e in events] == ["task.created", "task.blocked"]

    await _run(monkeypatch, session_factory, parent_id)

    async with session_factory() as session:
        task = await session.get(Task, child_id)
        assert task is not None
        assert task.status == "queued"
        outbox = list((await session.execute(select(OutboxEvent))).scalars().all())
        child_events = [
            o for o in outbox if (o.payload or {}).get("task_id") == child_id
        ]
        assert len(child_events) == 1
        assert child_events[0].status == "pending"

    await _run(monkeypatch, session_factory, child_id)
    async with session_factory() as session:
        task = await session.get(Task, child_id)
        assert task is not None
        assert task.status == "success"
        timeline = await client.get(f"/api/v1/tasks/{child_id}/events")
        types = [e["event_type"] for e in timeline.json()["events"]]
        assert "task.unblocked" in types


@pytest.mark.asyncio
async def test_dag_parent_peer_pending_keeps_child_blocked(
    client, session_factory, monkeypatch
) -> None:
    p1 = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    p2 = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    child = await client.post(
        "/api/v1/tasks",
        json={
            "task_type": "echo",
            "depends_on": [p1.json()["task"]["id"], p2.json()["task"]["id"]],
        },
    )
    child_id = child.json()["task"]["id"]

    await _run(monkeypatch, session_factory, p1.json()["task"]["id"])
    async with session_factory() as session:
        task = await session.get(Task, child_id)
        assert task is not None
        assert task.status == "created"


@pytest.mark.asyncio
async def test_dag_parent_failure_cancels_child(client, session_factory, monkeypatch) -> None:
    parent = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 1, "payload": {"reason": "boom"}},
    )
    parent_id = parent.json()["task"]["id"]
    child = await client.post(
        "/api/v1/tasks", json={"task_type": "echo", "depends_on": [parent_id]}
    )
    child_id = child.json()["task"]["id"]

    await _run(monkeypatch, session_factory, parent_id)

    async with session_factory() as session:
        task = await session.get(Task, child_id)
        assert task is not None
        assert task.status == "cancelled"
    timeline = (await client.get(f"/api/v1/tasks/{child_id}/events")).json()["events"]
    cancel_event = [e for e in timeline if e["event_type"] == "task.cancelled"][-1]
    assert cancel_event["metadata"]["reason"] == "parent_terminal_non_success"


# --- webhooks ---


@pytest.mark.asyncio
async def test_webhook_delivery_lifecycle(
    client, session_factory, monkeypatch, hook_server
) -> None:
    created = await client.post(
        "/api/v1/webhooks",
        json={"name": "h1", "url": hook_server, "events": ["task.queued"], "secret": "s3cr3t"},
    )
    assert created.status_code == 201
    sub_id = created.json()["id"]

    task = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    task_id = task.json()["task"]["id"]

    async with session_factory() as session:
        deliveries = list((await session.execute(select(WebhookDelivery))).scalars())
        assert len(deliveries) == 1
        assert deliveries[0].status == "pending"
        assert deliveries[0].event_type == "task.queued"
        assert deliveries[0].task_id == task_id

    _patch_webhooks(monkeypatch, session_factory)
    sent = await webhooks_service.dispatch_due()
    assert sent == 1

    async with session_factory() as session:
        delivery = (await session.execute(select(WebhookDelivery))).scalar_one()
        assert delivery.status == "sent"
        assert delivery.attempts == 0
        assert delivery.last_error is None

    assert len(RecordingHandler.records) == 1
    record = RecordingHandler.records[0]
    assert record["body"]["event"] == "task.queued"
    assert record["body"]["task"]["id"] == task_id
    assert record["event"] == "task.queued"
    assert record["signature"].startswith("sha256=")

    test_result = await client.post(f"/api/v1/webhooks/{sub_id}/test")
    assert test_result.status_code == 200
    assert test_result.json()["ok"] is True
    assert test_result.json()["status_code"] == 200
    assert len(RecordingHandler.records) == 2
    assert RecordingHandler.records[-1]["body"]["event"] == "test.ping"


@pytest.mark.asyncio
async def test_webhook_inactive_skips_delivery(
    client, session_factory, hook_server
) -> None:
    await client.post(
        "/api/v1/webhooks",
        json={"name": "off", "url": hook_server, "events": ["task.queued"]},
    )
    r = await client.get("/api/v1/webhooks")
    sub_id = r.json()[0]["id"]
    await client.patch(f"/api/v1/webhooks/{sub_id}", json={"is_active": False})

    await client.post("/api/v1/tasks", json={"task_type": "echo"})
    async with session_factory() as session:
        count = len(list((await session.execute(select(WebhookDelivery))).scalars()))
        assert count == 0


@pytest.mark.asyncio
async def test_webhook_delete_and_unknown_events(client) -> None:
    unknown = await client.post(
        "/api/v1/webhooks",
        json={"name": "x", "url": "http://127.0.0.1:9/h", "events": ["task.lol"]},
    )
    assert unknown.status_code == 400

    created = await client.post(
        "/api/v1/webhooks",
        json={"name": "x", "url": "http://127.0.0.1:9/h", "events": ["*"]},
    )
    sub_id = created.json()["id"]
    deleted = await client.delete(f"/api/v1/webhooks/{sub_id}")
    assert deleted.status_code == 204
    listed = await client.get("/api/v1/webhooks")
    assert listed.json() == []


@pytest.mark.asyncio
async def test_webhook_requires_admin(client) -> None:
    reg = await client.post(
        "/api/v1/auth/register", json={"username": "viewer1", "password": "pass12345"}
    )
    assert reg.status_code == 201
    login = await client.post(
        "/api/v1/auth/login", json={"username": "viewer1", "password": "pass12345"}
    )
    token = login.json()["access_token"]
    async with await _new_client() as viewer:
        viewer.headers["Authorization"] = f"Bearer {token}"
        for path in (
            "/api/v1/webhooks",
            "/api/v1/webhooks/deliveries",
            "/api/v1/audit",
        ):
            assert (await viewer.get(path)).status_code == 403


# --- API-токени ---


@pytest.mark.asyncio
async def test_api_token_auth_and_revoke(client) -> None:
    created = await client.post("/api/v1/auth/tokens", json={"name": "ci"})
    assert created.status_code == 201
    raw = created.json()["token"]
    assert raw.startswith(settings.api_token_prefix)
    token_id = created.json()["api_token"]["id"]

    async with await _new_client() as api:
        api.headers["Authorization"] = f"Bearer {raw}"
        ok = await api.get("/api/v1/tasks")
        assert ok.status_code == 200

    revoked = await client.delete(f"/api/v1/auth/tokens/{token_id}")
    assert revoked.status_code == 204

    async with await _new_client() as api:
        api.headers["Authorization"] = f"Bearer {raw}"
        denied = await api.get("/api/v1/tasks")
        assert denied.status_code == 401

    listed = await client.get("/api/v1/auth/tokens")
    items = listed.json()["items"]
    assert len(items) == 1
    assert items[0]["revoked_at"] is not None


# --- audit ---


@pytest.mark.asyncio
async def test_audit_log_records_task_create_and_filters(client) -> None:
    await client.post("/api/v1/tasks", json={"task_type": "echo"})
    rows = await client.get("/api/v1/audit?action=task.create")
    assert rows.status_code == 200
    body = rows.json()
    assert body["total"] >= 1
    assert body["items"][0]["action"] == "task.create"
    assert body["items"][0]["actor_username"] == "admin"

    by_type = await client.get("/api/v1/audit?resource_type=task")
    assert by_type.json()["total"] >= 1

    search = await client.get("/api/v1/audit?q=task.create")
    assert search.json()["total"] >= 1

    viewer_list = await client.get("/api/v1/audit?limit=1")
    assert viewer_list.json()["total"] >= 1
    assert len(viewer_list.json()["items"]) == 1


# --- retention / GC ---


@pytest.mark.asyncio
async def test_retention_purges_old_terminal_tasks_and_logs(
    client, session_factory, monkeypatch
) -> None:
    monkeypatch.setattr(settings, "retention_done_tasks_days", 30)
    monkeypatch.setattr(settings, "retention_dlq_tasks_days", 30)
    monkeypatch.setattr(settings, "retention_outbox_sent_days", 7)
    monkeypatch.setattr(settings, "retention_audit_days", 365)

    fresh = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    fresh_id = fresh.json()["task"]["id"]
    await _run(monkeypatch, session_factory, fresh_id)

    old_ok = await client.post("/api/v1/tasks", json={"task_type": "echo"})
    old_ok_id = old_ok.json()["task"]["id"]
    await _run(monkeypatch, session_factory, old_ok_id)

    old_dlq = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fail", "max_attempts": 1, "payload": {"reason": "x"}},
    )
    old_dlq_id = old_dlq.json()["task"]["id"]
    await _run(monkeypatch, session_factory, old_dlq_id)

    now = datetime.now(UTC)
    async with session_factory() as session:
        for task_id in (old_ok_id, old_dlq_id):
            task = await session.get(Task, task_id)
            assert task is not None
            task.finished_at = now - timedelta(days=40)
        audit_rows = list((await session.execute(select(AuditLog))).scalars())
        assert audit_rows
        old_audit = audit_rows[-1]
        old_audit_id = old_audit.id
        old_audit.created_at = now - timedelta(days=400)
        outbox_rows = list((await session.execute(select(OutboxEvent))).scalars())
        old_outbox = outbox_rows[-1]
        old_outbox_id = old_outbox.id
        old_outbox.processed_at = now - timedelta(days=40)
        await session.commit()

    _patch_scheduler(monkeypatch, session_factory)
    purged = await retry_scheduler.purge_old_data()
    assert purged >= 4

    async with session_factory() as session:
        assert (await session.get(Task, fresh_id)) is not None
        assert (await session.get(Task, old_ok_id)) is None
        assert (await session.get(Task, old_dlq_id)) is None
        assert (await session.get(AuditLog, old_audit_id)) is None
        assert (await session.get(OutboxEvent, old_outbox_id)) is None
        remaining = list((await session.execute(select(OutboxEvent))).scalars())
        assert any(o.processed_at is None for o in remaining)
        still_fresh = await session.get(Task, fresh_id)
        assert still_fresh is not None
        assert still_fresh.status == "success"
