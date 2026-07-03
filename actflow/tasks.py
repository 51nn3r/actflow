from __future__ import annotations

from typing import Any, Callable

from .core import TaskResult
from .task import Task


class Input(Task):
    """Graph entry point: forwards the received value through the 'next' link."""

    def execute(self, value: Any) -> dict:
        return {"next": value}


class Terminal(Task):
    """Collects the received value as a graph output."""

    def execute(self, value: Any) -> list[TaskResult]:
        return [TaskResult(value, None, None)]


class Tap(Task):
    """Runs a side-effect action then forwards the value through the 'next' link.
    action(task, value) — may call task.stop() to steer the executor."""

    def __init__(self, action: Callable[["Task", Any], None], **kwargs: Any):
        super().__init__(**kwargs)
        self.action = action

    def execute(self, value: Any) -> dict:
        self.action(self, value)
        return {"next": value}
