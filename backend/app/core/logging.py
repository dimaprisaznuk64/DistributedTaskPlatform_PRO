from __future__ import annotations

import json
import logging
import sys

from app.core.config import settings

_RESERVED_KEYS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
    "asctime",
    "message",
}


class JsonFormatter(logging.Formatter):
    """Однорядковий JSON-лог для машинного розбору (Grafana/pipelines)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S.%f%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key in _RESERVED_KEYS:
                continue
            if isinstance(value, str | int | float | bool | dict | list):
                payload[key] = value
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(json_format: bool | None = None, level: str | None = None) -> None:
    """Налаштовує кореневий логер: JSON- або текстовий формат."""
    level = (level or settings.log_level).upper()
    use_json = settings.log_json if json_format is None else json_format
    handler = logging.StreamHandler(sys.stdout)
    plain = logging.Formatter("%(levelname)s %(name)s: %(message)s")
    handler.setFormatter(JsonFormatter() if use_json else plain)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
