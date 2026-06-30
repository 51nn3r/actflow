from __future__ import annotations

import inspect
from collections import deque

from .core import Collected, Ready, Wait


class ExecutionControllerInterface:
    """Runs the task body. Subclass to redirect execution (e.g. to a remote worker)."""

    async def run(self, task, data: dict):
        raise NotImplementedError


class LocalExecutionController(ExecutionControllerInterface):
    """Default: runs task.execute(**data) in-process."""

    async def run(self, task, data: dict):
        results = task.execute(**data)
        if inspect.iscoroutine(results):
            results = await results
        return results


class InputControllerInterface:
    """Interface for input controllers: readiness gating and slot collection."""

    def offer(self, packet) -> Ready | Wait:
        raise NotImplementedError

    def poll(self) -> Ready | Wait:
        raise NotImplementedError

    def collect(self) -> Collected:
        raise NotImplementedError


class InputController(InputControllerInterface):
    """FIFO: one queue per slot. Routes by label; unknown → first free slot."""

    def __init__(self, labels: tuple[str, ...]):
        self.labels = labels or ("in",)
        self.queues: dict[str, deque] = {lab: deque() for lab in self.labels}
        self._bound: dict[str, str] = {}

    def offer(self, packet) -> Ready | Wait:
        self._route(packet)
        return self.poll()

    def _route(self, packet) -> None:
        if packet.label in self.queues:
            self.queues[packet.label].append(packet)
            return

        slot = self._bound.get(packet.label) or self._free_slot()
        self._bound[packet.label] = slot
        self.queues[slot].append(packet)

    def _free_slot(self) -> str:
        for lab in self.labels:
            if not self.queues[lab] and lab not in self._bound.values():
                return lab

        return self.labels[0]

    def poll(self) -> Ready | Wait:
        for lab in self.labels:
            if not self.queues[lab]:
                return Wait()

        return Ready()

    def collect(self) -> Collected:
        return Collected(data={lab: self.queues[lab].popleft().value for lab in self.labels})


class OrderedInputController(InputController):
    """Delivers packets in strict ascending order of their value["idx"] field.
    Out-of-order arrivals are held until their turn; mark carries {"idx": n}."""

    def __init__(self, labels: tuple[str, ...]):
        super().__init__(labels)
        self._slot = self.labels[0]
        self._next = 0
        self._held: dict = {}

    def offer(self, packet) -> Ready | Wait:
        self._held[packet.value["idx"]] = packet.value
        return self.poll()

    def poll(self) -> Ready | Wait:
        return Ready() if self._next in self._held else Wait()

    def collect(self) -> Collected:
        item = self._held.pop(self._next)
        idx = self._next
        self._next += 1
        return Collected(data={self._slot: item}, mark={"idx": idx})


class OutputControllerInterface:
    """Interface for output controllers: label-stamping and result dispatch."""

    def emit(self, results, mark: dict | None) -> list:
        raise NotImplementedError


class OutputController(OutputControllerInterface):
    """Default: stamps source label onto outgoing packets."""

    def __init__(self, type_label: str):
        self.type_label = type_label

    def emit(self, results, mark: dict | None) -> list:
        out = []
        for r in results:
            label = r.label if r.label is not None else self.type_label
            out.append((r.value, r.node, label))

        return out