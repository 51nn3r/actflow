from __future__ import annotations

from .core import TaskResult
from .task import Task


class Input(Task):
    """Graph entry point: forwards the received value through the 'next' link."""

    in_labels = ("value",)

    def execute(self, value) -> dict:
        return {"next": value}


class Terminal(Task):
    """Collects the received value as a graph output."""

    in_labels = ("value",)

    def execute(self, value) -> list[TaskResult]:
        return [TaskResult(value, None, None)]


class Tap(Task):
    """Runs a side-effect action then forwards the value through the 'next' link.
    action(task, value) — may call task.stop() to steer the executor."""

    in_labels = ("value",)

    def __init__(self, action, **kwargs):
        super().__init__(**kwargs)
        self.action = action

    def execute(self, value) -> dict:
        self.action(self, value)
        return {"next": value}