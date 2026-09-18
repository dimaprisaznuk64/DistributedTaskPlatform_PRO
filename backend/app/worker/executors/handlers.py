from __future__ import annotations

import asyncio


class EchoHandler:
    name = "echo"
    description = "Повертає передане повідомлення"

    async def execute(self, payload: dict) -> dict:
        return {"message": payload.get("message", ""), "task_type": self.name}


class SleepHandler:
    name = "sleep"
    description = "Спить задану кількість секунд (0..30) і повертає час"

    async def execute(self, payload: dict) -> dict:
        raw = payload.get("seconds", 0)
        try:
            seconds = float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"seconds має бути числом, отримано {raw!r}") from exc
        seconds = max(0.0, min(30.0, seconds))
        await asyncio.sleep(seconds)
        return {"slept": seconds}


class FailHandler:
    name = "fail"
    description = "Навмисно падає — для демонстрації помилок та повторів"

    async def execute(self, payload: dict) -> dict:
        reason = payload.get("reason", "demo-помилка")
        raise RuntimeError(f"Запит на помилку: {reason}")
