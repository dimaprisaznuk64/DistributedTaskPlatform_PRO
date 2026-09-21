from __future__ import annotations

import time

from prometheus_client import Counter, Gauge, Histogram, generate_latest
from starlette.routing import Match

tasks_created_total = Counter("tasks_created_total", "Створено задач (усього)")
attempts_total = Counter(
    "attempts_total",
    "Виконані спроби за результатом (success/failed/dead_letter/retry_scheduled)",
    ["outcome"],
)
retries_total = Counter("retries_total", "Повернення задач у чергу координатором")
lease_timeouts_total = Counter(
    "lease_timeouts_total", "Задачі, повернуті за простроченим lease"
)
task_duration_seconds = Histogram(
    "task_duration_seconds",
    "Тривалість виконання задачі",
    ["task_type"],
)
tasks_status_gauge = Gauge("tasks_status", "Поточна кількість задач за статусом", ["status"])
workers_active = Gauge("workers_active", "Живі воркери")
workers_total = Gauge("workers_total", "Усього зареєстрованих воркерів")
outbox_pending = Gauge("outbox_pending", "Невідправлені outbox-події")
rabbitmq_queue_depth = Gauge("rabbitmq_queue_depth", "Повідомлення у RabbitMQ-черзі")
webhook_deliveries_total = Counter(
    "webhook_deliveries_total", "Webhook-доставки за результатом", ["outcome"]
)
retention_deleted_total = Counter(
    "retention_deleted_total", "Видалено рядків GC за таблицею", ["table"]
)
http_requests_total = Counter(
    "http_requests_total", "HTTP-запити (метод, шлях, статус)", ["method", "path", "status"]
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "Тривалість HTTP-запитів", ["method", "path"]
)


def render_metrics() -> bytes:
    return generate_latest()


class PrometheusMiddleware:
    """Підраховує HTTP-запити та їх тривалість (шляхи нормалізуються до шаблонів)."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        start = time.perf_counter()
        status = {"code": 500}
        path_template = self._path_template(scope)

        async def wrapped_send(message) -> None:
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, wrapped_send)
        finally:
            method = scope.get("method", "GET")
            code = str(status["code"])
            duration = time.perf_counter() - start
            http_requests_total.labels(method=method, path=path_template, status=code).inc()
            http_request_duration_seconds.labels(method=method, path=path_template).observe(
                duration
            )

    def _path_template(self, scope) -> str:
        try:
            for route in self.app.router.routes:
                match, _ = route.matches(scope)
                if match == Match.FULL:
                    return route.path
        except Exception:
            pass
        return scope.get("path", "")
