from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .node import Node


_current_node: ContextVar = ContextVar("_current_node", default=None)
_current_handle: ContextVar = ContextVar("_current_handle", default=None)


@dataclass(frozen=True)
class Packet:
    """Immutable envelope on the wire: a value plus its source label."""

    value: Any
    label: str


@dataclass(frozen=True)
class Collected:
    """Result of collect(): dequeued inputs + optional mark for OutputController."""

    data: dict
    mark: dict | None = None


@dataclass(frozen=True)
class TaskResult:
    """Addressed body result: value, target node (None = graph output), optional label."""

    value: Any
    node: "Node | None"
    label: str | None = None


class Verdict:
    """Readiness answer an input controller returns for a node."""


@dataclass(frozen=True)
class Ready(Verdict):
    """Run the node now."""


@dataclass(frozen=True)
class Wait(Verdict):
    """Not ready; wake only when new data arrives."""


@dataclass(frozen=True)
class WaitUntil(Verdict):
    """Not ready; wake no later than deadline (monotonic seconds)."""

    deadline: float
