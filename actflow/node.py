from __future__ import annotations

from .control import (
    InputControllerInterface,
    OutputControllerInterface,
    ExecutionControllerInterface,
    LocalExecutionController,
)
from .core import _current_node, _current_ctrl


class LinkRef:
    """Named output socket; used to wire graph edges via >>."""

    def __init__(self, node: Node, name: str):
        self.node = node
        self.name = name

    def __rshift__(self, target: Node | LinkRef) -> Node:
        target_node = target.node if isinstance(target, LinkRef) else target
        self.node.links[self.name] = target_node
        return target_node


class Node:
    """Task's slot in the graph: controllers, links, per-node memory, and execution."""

    def __init__(
        self,
        task,
        input_controller: InputControllerInterface,
        output_controller: OutputControllerInterface,
        execution_controller: ExecutionControllerInterface | None = None,
        name: str = "",
    ):
        self.task = task
        self.name = name or type(task).__name__
        self.input_controller = input_controller
        self.output_controller = output_controller
        self.execution_controller = execution_controller or LocalExecutionController()
        self.links: dict[str, Node] = {}
        self.memory: dict = {}

    async def run(self, ctrl) -> tuple:
        """Collect inputs, run via execution_controller, return (results, mark)."""
        collected = self.input_controller.collect()
        tok_n = _current_node.set(self)
        tok_c = _current_ctrl.set(ctrl)
        try:
            results = await self.execution_controller.run(self.task, collected.data)
            return results, collected.mark
        finally:
            _current_node.reset(tok_n)
            _current_ctrl.reset(tok_c)

    def link(self, name: str, target: Node) -> Node:
        self.links[name] = target
        return self

    def __rshift__(self, target: Node | LinkRef) -> Node:
        return self["next"].__rshift__(target)

    def __getitem__(self, name: str) -> LinkRef:
        return LinkRef(self, name)

    def __repr__(self) -> str:
        return f"<Node {self.name}>"