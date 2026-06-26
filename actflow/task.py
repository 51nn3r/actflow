from __future__ import annotations

from .node import Node


class Task:
    """Unit of computation. Override execute.

    execute(inputs, ctx) → list[TaskResult]
    inputs — {label: value} dict assembled by the input controller
    The body may be a plain function (sync) or a coroutine (async)."""

    in_labels: tuple = ()
    type_label: str = ""

    def execute(self, inputs: dict, ctx) -> list:
        raise NotImplementedError

    def __call__(self, name="") -> Node:
        labels = self.in_labels
        type_label = self.type_label or type(self).__name__

        return Node(self, labels, type_label, name=name)
