from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket

from app.core.config import settings
from app.db.session import session_factory
from app.services import events as events_service
from app.services import tasks as tasks_service
from app.services import workers as workers_service
from app.worker.broker import consumer
from app.worker.executors import registry
from app.worker.executors.registry import UnknownTaskTypeError
from app.worker.services import attempts as attempts_service
from app.worker.services import scheduler as coordinator

logger = logging.getLogger(__name__)

WORKER_ID = f"{settings.worker_id}-{os.getpid()}"
HOSTNAME = socket.gethostname()


async def _dispatch(task_type: str, payload: dict) -> dict:
    handler = registry.get_handler(task_type)
    if handler is None:
        available = ", ".join(registry.registered_types())
        raise UnknownTaskTypeError(f"Невідомий тип задачі: {task_type!r}. Доступно: {available}")
    return await handler.execute(payload)


async def _renew_lease_loop(task_id: int) -> None:
    """Подовжує lease, поки виконується задача (окрема сесія)."""
    while True:
        await asyncio.sleep(settings.task_lease_seconds / 3)
        try:
            async with session_factory() as session:
                ok = await attempts_service.renew_lease(
                    session, task_id, WORKER_ID, settings.task_lease_seconds
                )
                await session.commit()
                if not ok:
                    return
        except Exception:
            logger.exception("Помилка продовження lease задачі %s", task_id)
            return


async def handle_message(message) -> None:
    """Обробляє одне повідомлення: with гарантує ack/nack у будь-якому випадку."""
    try:
        body = consumer.decode_payload(message)
        task_id = int(body.get("task_id"))
        async with session_factory() as session:
            acquired = await attempts_service.try_acquire_lease(
                session, task_id, WORKER_ID, settings.task_lease_seconds
            )
            if not acquired:
                await message.ack()
                logger.info("Задача %s не отримала lease, пропускаю", task_id)
                return

            task = await tasks_service.get_task(session, task_id)
            if task is None:
                await message.ack()
                return

            attempt = await attempts_service.start_attempt(session, task)
            attempt.worker_id = WORKER_ID
            await session.commit()
            events_service.schedule_task_event("task.running", task)

            renewer = asyncio.create_task(_renew_lease_loop(task_id))
            try:
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
                events_service.schedule_task_event("task.updated", task)
            finally:
                renewer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await renewer
        await message.ack()
    except Exception:
        logger.exception("Аварійна помилка при обробці повідомлення")
        try:
            await message.nack(requeue=False)
        except Exception:
            logger.exception("Не вдалось nack повідомлення")


async def run_worker() -> None:
    logger.info("Worker %s запущено", WORKER_ID)
    async for message in consumer.consume_tasks():
        await handle_message(message)


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


async def run_heartbeat() -> None:
    logger.info("Heartbeat %s запущено (кожні %.1fс)", WORKER_ID, settings.worker_heartbeat_seconds)
    while True:
        try:
            async with session_factory() as session:
                worker = await workers_service.heartbeat(session, WORKER_ID)
                if worker is None:
                    await workers_service.register_worker(
                        session, WORKER_ID, hostname=HOSTNAME, pid=os.getpid()
                    )
                    published = True
                else:
                    published = False
                await session.commit()
            if published:
                events_service.schedule_worker_event("worker.alive", WORKER_ID)
        except Exception:
            logger.exception("Heartbeat: помилка")
        await asyncio.sleep(settings.worker_heartbeat_seconds)


async def run_coordinator() -> None:
    logger.info("Координатор запущено (кожні %.1fс)", settings.retry_scheduler_poll_seconds)
    while True:
        try:
            requeued = await coordinator.requeue_due_tasks()
            recovered = await coordinator.recover_stuck_tasks()
            if requeued or recovered:
                logger.info("У чергу повернуто: %s, відновлено: %s", requeued, recovered)
        except Exception:
            logger.exception("Координатор: помилка тику")
        await asyncio.sleep(settings.retry_scheduler_poll_seconds)


async def main() -> None:
    await asyncio.gather(
        _run_worker_with_reconnect(),
        run_heartbeat(),
        run_coordinator(),
    )


if __name__ == "__main__":
    from app.core.logging import setup_logging

    setup_logging()
    asyncio.run(main())
