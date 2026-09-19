from __future__ import annotations

import asyncio
import logging

from app.core.config import settings
from app.db.session import session_factory
from app.services import tasks as tasks_service
from app.worker.broker import consumer
from app.worker.executors import registry
from app.worker.executors.registry import UnknownTaskTypeError
from app.worker.services import attempts as attempts_service
from app.worker.services import scheduler as retry_scheduler

logger = logging.getLogger(__name__)


async def _dispatch(task_type: str, payload: dict) -> dict:
    handler = registry.get_handler(task_type)
    if handler is None:
        available = ", ".join(registry.registered_types())
        raise UnknownTaskTypeError(f"Невідомий тип задачі: {task_type!r}. Доступно: {available}")
    return await handler.execute(payload)


async def handle_message(message) -> None:
    """Обробляє одне повідомлення: with гарантує ack/nack у будь-якому випадку."""
    try:
        body = consumer.decode_payload(message)
        task_id = body.get("task_id")
        async with session_factory() as session:
            task = await tasks_service.get_task(session, int(task_id))
            if task is None or task.status != "queued":
                await message.ack()
                logger.info(
                    "Задача %s не в черзі (%s), пропускаю",
                    task_id,
                    getattr(task, "status", None),
                )
                return

            attempt = await attempts_service.start_attempt(session, task)
            attempt.worker_id = settings.worker_id
            await session.commit()

            try:
                result = await asyncio.wait_for(
                    _dispatch(task.task_type, task.payload),
                    timeout=settings.task_execution_timeout_seconds,
                )
            except TimeoutError:
                await attempts_service.record_failure(
                    session,
                    task,
                    attempt,
                    f"Таймаут виконання ({settings.task_execution_timeout_seconds}с)",
                    retryable=True,
                )
            except UnknownTaskTypeError as exc:
                await attempts_service.record_failure(
                    session, task, attempt, str(exc), retryable=False
                )
            except Exception as exc:
                logger.exception("Задача %s впала", task.id)
                await attempts_service.record_failure(
                    session, task, attempt, str(exc), retryable=True
                )
            else:
                await attempts_service.record_success(session, task, attempt, result)

            await session.commit()
        await message.ack()
    except Exception:
        logger.exception("Аварійна помилка при обробці повідомлення")
        try:
            await message.nack(requeue=False)
        except Exception:
            logger.exception("Не вдалось nack повідомлення")


async def run_worker() -> None:
    logger.info("Worker %s запущено", settings.worker_id)
    async for message in consumer.consume_tasks():
        await handle_message(message)


async def run_retry_scheduler() -> None:
    logger.info("Retry-планувальник запущено (кожні %.1fс)", settings.retry_scheduler_poll_seconds)
    while True:
        try:
            requeued = await retry_scheduler.requeue_due_tasks()
            if requeued:
                logger.info("Повернуто в чергу задач: %s", requeued)
        except Exception:
            logger.exception("Retry-планувальник: помилка тику")
        await asyncio.sleep(settings.retry_scheduler_poll_seconds)


async def _run_worker_with_reconnect() -> None:
    max_backoff = 10.0
    backoff = 1.0
    while True:
        try:
            await run_worker()
        except Exception:
            logger.exception("Зв'язок з RabbitMQ втрачено, повтор через %.1fс", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


async def main() -> None:
    await asyncio.gather(_run_worker_with_reconnect(), run_retry_scheduler())


if __name__ == "__main__":
    logging.basicConfig(level=settings.log_level)
    asyncio.run(main())
