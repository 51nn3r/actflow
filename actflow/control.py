from __future__ import annotations

from collections import deque

from .core import Collected, Ready, Wait


class InputController:
    """One FIFO queue per input slot. Routes packets by label; unknown → first free slot."""

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


class OutputController:
    """Stamps outgoing packets with the node's source label."""

    def __init__(self, type_label: str):
        self.type_label = type_label

    def emit(self, results, mark: dict | None) -> list:
        out = []
        for r in results:
            label = r.label if r.label is not None else self.type_label
            out.append((r.value, r.node, label))

        return out