from __future__ import annotations

from typing import Any, Callable

from .core import Downstream
from .task import Task


class Input(Task):
    """Graph entry point: forwards the received value on the 'next' output."""

    def execute(self, value: Any) -> Downstream:
        return Downstream(value)


class Tap(Task):
    """Runs a side-effect action, then forwards the value on the 'next' output."""

    def __init__(self, action: Callable[[Task, Any], None], **kwargs: Any):
        super().__init__(**kwargs)
        self.action = action

    def execute(self, value: Any) -> Downstream:
        self.action(self, value)
        return Downstream(value)
