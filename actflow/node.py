from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .control import (
    InputControllerInterface,
    OutputControllerInterface,
    ExecutionControllerInterface,
    LocalExecutionController,
)
from .core import Collected, Packet, TaskResult, Verdict, _current_node, _current_handle

if TYPE_CHECKING:
    from .task import Task
    from .executor import ExecutorHandle


class LinkRef:
    """Named output socket; used to wire graph edges via >>."""

    def __init__(self, node: Node, name: str):
        self.node = node
        self.name = name

    def __rshift__(self, target: Node | LinkRef) -> Node:
        """Wire this socket to target and return target for chaining."""
        target_node = target.node if isinstance(target, LinkRef) else target
        self.node.links[self.name] = target_node
        return target_node


class Node:
    """Task's slot in the graph: controllers, links, memory, execution.
    Facade over its controllers — the executor talks to the node, never the controllers."""

    def __init__(
        self,
        task: Task,
        input_controller: InputControllerInterface,
        output_controller: OutputControllerInterface,
        execution_controller: ExecutionControllerInterface | None = None,
        name: str = "",
    ):
        self.task = task
        self.name = name or type(task).__name__
        self.source_label = self.name
        self.input_controller = input_controller
        self.output_controller = output_controller
        self.execution_controller = execution_controller or LocalExecutionController()
        self.links: dict[str, Node] = {}
        self.memory: dict = {}

    def offer(self, packet: Packet) -> Verdict:
        """Map the packet's source label to a task slot (hop 1), then hand to the controller.
        No input_map = pass through; source label absent from the map = drop."""
        input_map = self.task.input_map
        if input_map is not None:
            slot = input_map.get(packet.label)
            if slot is None:
                self.task.on_dropped(packet)
                return self.poll()

            packet = Packet(packet.value, slot)

        return self.input_controller.offer(packet)

    def poll(self) -> Verdict:
        """Re-ask the input controller whether the node is ready to run."""
        return self.input_controller.poll()

    def collect(self) -> Collected:
        """Pull the inputs the body will receive this tick."""
        return self.input_controller.collect()

    async def run(self, handle: ExecutorHandle) -> tuple:
        """Collect inputs, run via execution_controller, return (results, mark)."""
        collected = self.collect()
        tok_node = _current_node.set(self)
        tok_handle = _current_handle.set(handle)
        try:
            results = await self.execution_controller.run(self.task, collected.data)
            return results, collected.mark
        finally:
            _current_node.reset(tok_node)
            _current_handle.reset(tok_handle)

    def dispatch(self, results: Any, mark: dict | None) -> list:
        """Translate raw body results into (value, target, label) triples.

        Owns the default source label; a None target routes to the graph output."""
        if results is None:
            return []

        if isinstance(results, dict):
            output_map = self.task.output_map or {}
            return [
                (value,
                 None if link_name is None else self.links[link_name],
                 output_map.get(link_name, self.source_label))
                for link_name, value in results.items()
            ]

        if isinstance(results, TaskResult):
            results = [results]

        out = []
        for value, target, label in self.output_controller.emit(results, mark):
            out.append((value, target, label if label is not None else self.source_label))

        return out

    def link(self, name: str, target: Node) -> Node:
        """Wire the named output socket to target; returns self for chaining."""
        self.links[name] = target
        return self

    def __rshift__(self, target: Node | LinkRef) -> Node:
        """Wire the default 'next' socket to target."""
        return self["next"].__rshift__(target)

    def __getitem__(self, name: str) -> LinkRef:
        return LinkRef(self, name)

    def __repr__(self) -> str:
        return f"<Node {self.name}>"
