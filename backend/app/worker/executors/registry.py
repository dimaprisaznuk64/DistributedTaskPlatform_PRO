from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class TaskHandler(Protocol):
    name: str
    description: str

    async def execute(self, payload: dict) -> dict: ...


class UnknownTaskTypeError(Exception):
    pass


_REGISTRY: dict[str, TaskHandler] = {}


def register(handler: TaskHandler) -> None:
    _REGISTRY[handler.name] = handler


def get_handler(task_type: str) -> TaskHandler | None:
    return _REGISTRY.get(task_type)


def registered_types() -> list[str]:
    return sorted(_REGISTRY)
