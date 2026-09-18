from app.worker.executors.handlers import EchoHandler, FailHandler, SleepHandler
from app.worker.executors.registry import get_handler, register

for _handler in (EchoHandler(), SleepHandler(), FailHandler()):
    register(_handler)

__all__ = ["get_handler"]
