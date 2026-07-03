from __future__ import annotations

import inspect
from collections import deque
from typing import TYPE_CHECKING, Any

from .core import Collected, Packet, Ready, TaskResult, Verdict, Wait

if TYPE_CHECKING:
    from .task import Task


class ExecutionControllerInterface:
    """Runs the task body. Subclass to redirect execution (e.g. to a remote worker)."""

    async def run(self, task: Task, data: dict) -> Any:
        """Execute the body with the collected inputs and return its raw result."""
        raise NotImplementedError


class LocalExecutionController(ExecutionControllerInterface):
    """Default: runs task.execute(**data) in-process."""

    async def run(self, task: Task, data: dict) -> Any:
        """Call execute in-process, awaiting it if it is a coroutine."""
        results = task.execute(**data)
        if inspect.iscoroutine(results):
            results = await results

        return results


class InputControllerInterface:
    """Interface for input controllers: readiness gating and slot collection."""

    def offer(self, packet: Packet) -> Verdict:
        """Accept an incoming packet and report readiness."""
        raise NotImplementedError

    def poll(self) -> Verdict:
        """Re-report readiness without new data (e.g. after a deadline)."""
        raise NotImplementedError

    def collect(self) -> Collected:
        """Dequeue the inputs the body will receive this tick."""
        raise NotImplementedError


class InputController(InputControllerInterface):
    """FIFO: one queue per slot. Routes by label; unknown → first free queue.
    slot_map renames task slots to internal queue names (hop 2), 1-to-1; default identity."""

    def __init__(self, labels: tuple[str, ...], slot_map: dict[str, str] | None = None):
        self.labels = labels or ("in",)
        if slot_map is not None:
            if set(slot_map) != set(self.labels):
                raise ValueError(f"slot_map keys must be the slots {self.labels}, got {slot_map}")

            if len(set(slot_map.values())) != len(slot_map):
                raise ValueError(f"slot_map must be 1-to-1, got {slot_map}")

        self._queue_of = slot_map or {slot: slot for slot in self.labels}
        self._slot_of = {queue: slot for slot, queue in self._queue_of.items()}
        self.queues: dict[str, deque] = {queue: deque() for queue in self._queue_of.values()}
        self._bound: dict[str, str] = {}

    def offer(self, packet: Packet) -> Verdict:
        """Route the packet into its queue, then report readiness."""
        self._route(packet)
        return self.poll()

    def _route(self, packet: Packet) -> None:
        """Send packet to the queue of its slot, else bind its label to a free queue."""
        queue = self._queue_of.get(packet.label)
        if queue is not None:
            self.queues[queue].append(packet)
            return

        queue = self._bound.get(packet.label) or self._free_queue()
        self._bound[packet.label] = queue
        self.queues[queue].append(packet)

    def _free_queue(self) -> str:
        """First empty, unbound queue; falls back to the first queue."""
        for queue in self.queues:
            if not self.queues[queue] and queue not in self._bound.values():
                return queue

        return next(iter(self.queues))

    def poll(self) -> Verdict:
        """Ready only when every queue holds at least one packet."""
        for queue in self.queues:
            if not self.queues[queue]:
                return Wait()

        return Ready()

    def collect(self) -> Collected:
        """Pop one value from each queue into the body's kwargs, keyed by slot."""
        return Collected(data={self._slot_of[queue]: self.queues[queue].popleft().value for queue in self.queues})


class OrderedInputController(InputController):
    """Delivers packets in strict ascending order of their value["idx"] field.
    Out-of-order arrivals are held until their turn; mark carries {"idx": n}."""

    def __init__(self, labels: tuple[str, ...]):
        super().__init__(labels)
        self._slot = self.labels[0]
        self._next = 0
        self._held: dict = {}

    def offer(self, packet: Packet) -> Verdict:
        """Stash the packet by its index, then report readiness."""
        self._held[packet.value["idx"]] = packet.value
        return self.poll()

    def poll(self) -> Verdict:
        """Ready only when the next expected index has arrived."""
        return Ready() if self._next in self._held else Wait()

    def collect(self) -> Collected:
        """Release the next-in-order value and advance the counter."""
        item = self._held.pop(self._next)
        idx = self._next
        self._next += 1
        return Collected(data={self._slot: item}, mark={"idx": idx})


class OutputControllerInterface:
    """Interface for output controllers: label-stamping and result dispatch."""

    def emit(self, results: list[TaskResult], mark: dict | None) -> list:
        """Turn body results into (value, target, label) triples for delivery."""
        raise NotImplementedError


class OutputController(OutputControllerInterface):
    """Default: passes results through untouched; the node stamps the source label."""

    def emit(self, results: list[TaskResult], mark: dict | None) -> list:
        """Return each result as a raw (value, target, label) triple; label may be None."""
        return [(r.value, r.node, r.label) for r in results]
