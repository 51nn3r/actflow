from __future__ import annotations

from collections import deque

from .core import Ready, Wait


class InputController:
    """Node input state: one FIFO queue per expected label.

    Packet routing: known label → its queue; unknown → first unbound slot.
    poll    — readiness check (default: every queue non-empty)
    collect — dequeue one item per slot and return with a receipt"""

    def __init__(self, labels: tuple[str, ...]):
        self.labels = labels or ("in",)
        self.queues: dict[str, deque] = {lab: deque() for lab in self.labels}
        self._bound: dict[str, str] = {}

    def offer(self, packet):
        self._route(packet)

        return self.poll()

    def _route(self, packet):
        if packet.label in self.queues:
            self.queues[packet.label].append(packet)

            return

        slot = self._bound.get(packet.label) or self._free_slot()
        self._bound[packet.label] = slot
        self.queues[slot].append(packet)

    def _free_slot(self):
        for lab in self.labels:
            if not self.queues[lab] and lab not in self._bound.values():
                return lab

        return self.labels[0]

    def poll(self):
        for lab in self.labels:
            if not self.queues[lab]:
                return Wait()

        return Ready()

    def collect(self):
        inputs = {lab: self.queues[lab].popleft().value for lab in self.labels}

        return inputs, None


class OutputController:
    """Stamps each result with the node's type label if the task didn't set one."""

    def __init__(self, type_label: str):
        self.type_label = type_label

    def emit(self, results, receipt):
        out = []
        for r in results:
            label = r.label if r.label is not None else self.type_label
            out.append((r.value, r.node, label))

        return out
