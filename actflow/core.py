from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .node import Node


@dataclass(frozen=True)
class NodePort:
    """One end of an edge. Input or output is decided by the side of >> it lands on."""

    node: "Node"
    name: str

    def __rshift__(self, target) -> "Node":
        """Wire this output to the target. Returns the target node, so >> chains."""
        return self.node.wire(self.name, target)


@dataclass(frozen=True)
class Collected:
    """Result of collect(): the body's arguments, plus metadata for the output side."""

    data: dict
    metadata: dict | None = None


@dataclass(frozen=True)
class ExecutionResult:
    """One value a body produced. The subclass says where it goes."""

    value: Any


@dataclass(frozen=True)
class Downstream(ExecutionResult):
    """Goes on through the graph, on this node's named output.
    shared: every active consumer gets it; otherwise one, picked by the node's policy."""

    output: str = "next"
    shared: bool = True


@dataclass(frozen=True)
class GraphOutput(ExecutionResult):
    """Leaves the graph."""


@dataclass(frozen=True)
class Delivery:
    """One value, already addressed: to its target, or out of the graph."""

    target: "NodePort | None"
    value: Any
    shared: bool = True
    outbound: bool = False


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