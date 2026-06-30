from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any


_current_node: ContextVar = ContextVar("_current_node", default=None)
_current_ctrl: ContextVar = ContextVar("_current_ctrl", default=None)


@dataclass(frozen=True)
class Packet:
    value: Any
    label: str


@dataclass(frozen=True)
class Collected:
    """Result of collect(): dequeued inputs + optional mark for OutputController."""

    data: dict
    mark: dict | None = None


@dataclass(frozen=True)
class TaskResult:
    value: Any
    node: "Node"
    label: str | None = None


class Verdict:
    pass


@dataclass(frozen=True)
class Ready(Verdict):
    pass


@dataclass(frozen=True)
class Wait(Verdict):
    pass


@dataclass(frozen=True)
class WaitUntil(Verdict):
    deadline: float
