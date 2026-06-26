from __future__ import annotations

from .core import TaskResult
from .control import InputController, OutputController


class Node:
    """Task's slot in the graph: controllers, body, and named outgoing links.
    Links are named so the task body stays reusable and holds no node references."""

    def __init__(self, task, labels, type_label, name=""):
        self.task = task
        self.name = name or type(task).__name__
        self.input = InputController(tuple(labels))
        self.output = OutputController(type_label)
        self.links: dict[str, "Node"] = {}

    def link(self, name, target) -> "Node":
        self.links[name] = target

        return self

    def __repr__(self):
        return f"<Node {self.name}>"


class Ctx:
    """Execution context passed to the task body for one tick.

    link(name)      — target node by link name
    to(name, value) — addressed result routed through that link
    out(value)      — emit value as a graph output
    memory          — node-local state persisted between ticks
    control         — executor control handles"""

    def __init__(self, node, control):
        self._node = node
        self.control = control

    @property
    def memory(self) -> dict:
        return self._node.__dict__.setdefault("_memory", {})

    def link(self, name) -> Node:
        return self._node.links[name]

    def to(self, name, value, label=None) -> TaskResult:
        return TaskResult(value, self._node.links[name], label)

    def out(self, value) -> TaskResult:
        # target None signals the executor to collect value as graph output
        return TaskResult(value, None, None)
