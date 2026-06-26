from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True)
class Packet:
    """Immutable value envelope; routable to multiple nodes without copying.

    value — payload the body works with;
    label — destination slot in the receiving node."""

    value: Any
    label: str

    def relabel(self, label: str) -> "Packet":
        return replace(self, label=label)


@dataclass(frozen=True)
class TaskResult:
    """Addressed result: data plus the target node and slot."""

    value: Any
    node: "Node"
    label: str | None = None  # None — use the source node's type label


class Verdict:
    pass


@dataclass(frozen=True)
class Ready(Verdict):
    """Node is ready to run."""


@dataclass(frozen=True)
class Wait(Verdict):
    """Not ready; wait for new incoming data."""


@dataclass(frozen=True)
class WaitUntil(Verdict):
    """Not ready, but wake no later than this deadline (batch timeout)."""

    deadline: float
