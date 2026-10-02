from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import aclosing
from threading import Lock
from typing import TYPE_CHECKING

from .control import (
    InputControllerInterface,
    ExecutionControllerInterface,
    OutputControllerInterface,
    RoutingPolicyInterface,
    ShortestQueue,
)
from .core import Collected, Delivery, NodePort, Verdict

if TYPE_CHECKING:
    from .task import Task


def _inputs_of(target: Node | NodePort) -> list[NodePort]:
    """Right side of >>: one named input, or every input of the node.

    Checked here because input names come from execute and are known at build
    time. Output names are not declared, so the left side cannot be checked.
    """
    if isinstance(target, NodePort):
        if target.name not in target.node.inputs:
            raise ValueError(
                f"{target.node.name} has no input {target.name!r}; its inputs are {target.node.inputs}"
            )

        return [target]

    return [NodePort(target, name) for name in target.inputs]


class Node:
    """A task's place in the graph: its ports, its wiring, its controllers."""

    def __init__(
        self,
        task: Task,
        inputs: tuple[str, ...],
        input_controller: InputControllerInterface,
        output_controller: OutputControllerInterface,
        execution_controller: ExecutionControllerInterface,
        name: str = "",
        isolated: bool = False,
        policy: RoutingPolicyInterface | None = None,
    ):
        self.task = task
        self.name = name or type(task).__name__
        self.inputs = tuple(inputs)
        # isolated: never two ticks at once
        self.isolated = isolated
        self.targets: dict[str, set[NodePort]] = {}
        # Tied to no output name, so they take every output there is.
        self.wildcard_targets: set[NodePort] = set()
        # Wiring can be added from outside the executor, from any thread.
        self.lock = Lock()
        # Controllers and policy arrive as prototypes; clear() takes fresh copies.
        self._prototypes = (
            input_controller,
            output_controller,
            execution_controller,
            policy or ShortestQueue(),
        )
        self.clear()

    def _targets_of(self, out_name: str) -> set[NodePort]:
        """Targets of this output, seeded from the wildcards on first sight.

        Call under the lock.
        """
        if out_name not in self.targets:
            self.targets[out_name] = set(self.wildcard_targets)

        return self.targets[out_name]

    def wire(self, out_name: str | None, target: Node | NodePort) -> Node:
        """Connect one output, or every output when out_name is None, to the target.

        Seeding a new name from the wildcards is what keeps a >> b and
        a["x"] >> b from delivering the same value twice.
        """
        ports = _inputs_of(target)
        with self.lock:
            if out_name is None:
                self.wildcard_targets.update(ports)
                for known in self.targets.values():
                    known.update(ports)
            else:
                self._targets_of(out_name).update(ports)

        return target.node if isinstance(target, NodePort) else target

    def targets_for(self, out_name: str) -> set[NodePort]:
        """Where a value emitted on this output goes. One lookup, never a scan."""
        with self.lock:
            return set(self._targets_of(out_name))

    def __rshift__(self, target: Node | NodePort) -> Node:
        """a >> b: every output of this node into the target."""
        return self.wire(None, target)

    def __getitem__(self, name: str) -> NodePort:
        return NodePort(self, name)

    def offer(self, delivery: Delivery) -> Verdict:
        """Take an incoming value and report readiness."""
        return self.input_controller.offer(delivery)

    def poll(self) -> Verdict:
        """Re-ask the input controller whether the node is ready to run."""
        return self.input_controller.poll()

    def collect(self) -> Collected:
        """Pull the inputs the body will receive this tick."""
        return self.input_controller.collect()

    def coverage(self) -> set[Node]:
        """This node and every node reachable from it along the wiring."""
        seen = {self}
        frontier = [self]
        while frontier:
            node = frontier.pop()
            with node.lock:
                ports = set(node.wildcard_targets).union(*node.targets.values())

            for port in ports:
                if port.node not in seen:
                    seen.add(port.node)
                    frontier.append(port.node)

        return seen

    def clear(self) -> None:
        """Back to initial state across this coverage: fresh controller copies
        from the prototypes for every node. Wiring stays. Call between runs
        only: clearing a coverage an Executor is running breaks that run."""
        for node in self.coverage():
            input_controller, output_controller, execution_controller, policy = node._prototypes
            node.input_controller = input_controller(node)
            node.output_controller = output_controller(node)
            node.execution_controller = execution_controller(node)
            node.policy = policy(node)

    def _twin(self) -> Node:
        """A fresh node from the same task and prototypes, no wiring."""
        input_controller, output_controller, execution_controller, policy = self._prototypes
        return Node(
            self.task,
            self.inputs,
            input_controller,
            output_controller,
            execution_controller,
            name=self.name,
            isolated=self.isolated,
            policy=policy,
        )

    def clone(self) -> Node:
        """A twin of this coverage: new nodes from the same tasks, same wiring,
        initial state. Prototype-held resources (a fiber pool) stay shared.

        Walk and snapshot are one pass, so an edge wired concurrently after a
        node's snapshot is simply absent from the twin.
        """
        twins = {self: self._twin()}
        frontier = [self]
        while frontier:
            node = frontier.pop()
            with node.lock:
                wildcard = set(node.wildcard_targets)
                named = {name: set(ports) for name, ports in node.targets.items()}

            ports = wildcard.union(*named.values()) if named else set(wildcard)
            for port in ports:
                if port.node not in twins:
                    twins[port.node] = port.node._twin()
                    frontier.append(port.node)

            twin = twins[node]
            for port in wildcard:
                twin.wire(None, NodePort(twins[port.node], port.name))

            for name, name_ports in named.items():
                for port in name_ports:
                    twin.wire(name, NodePort(twins[port.node], port.name))

        return twins[self]

    async def run(self, collected: Collected) -> AsyncIterator[Delivery]:
        """Execute on the collected inputs, hand each result to the output controller."""
        async with aclosing(self.execution_controller.run(self.task, collected.data)) as results:
            async for result in results:
                for delivery in await self.output_controller.dispatch(result, collected.metadata):
                    yield delivery

        await self.output_controller.close_tick(collected.metadata)

    def __repr__(self) -> str:
        return f"<Node {self.name}>"
