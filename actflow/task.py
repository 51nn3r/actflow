from __future__ import annotations

import functools
import inspect

from .control import InputController, OutputController
from .core import TaskResult, _current_node, _current_ctrl
from .node import Node


@functools.lru_cache(maxsize=None)
def _infer_in_labels(cls) -> tuple[str, ...]:
    sig = inspect.signature(cls.execute)
    return tuple(p for p in sig.parameters if p not in ("self", "ctx"))


class Task:
    """Unit of computation. Subclass and override execute().

    Return dict {link_name: value} from execute; None key = graph output.
    Use list[TaskResult] for fan-out or explicit label control.
    Body may be sync or async."""

    in_labels: tuple | None = None
    out_labels: tuple | None = None
    type_label: str = ""

    def __init__(self, *, label: str = "", input_controller=None, output_controller=None):
        self.label = label
        self._ic = input_controller
        self._oc = output_controller

    def execute(self, *args, **kwargs):
        raise NotImplementedError

    def __call__(self) -> Node:
        labels = self.in_labels if self.in_labels is not None else _infer_in_labels(type(self))
        source_label = self.type_label or (self.out_labels[0] if self.out_labels else None) or self.label or type(self).__name__
        ic = self._ic or InputController(labels)
        oc = self._oc or OutputController(source_label)
        return Node(self, ic, oc, name=source_label)

    @property
    def memory(self) -> dict:
        return _current_node.get().memory

    @property
    def links(self) -> dict[str, Node]:
        return _current_node.get().links

    def to(self, name: str, value, label=None) -> TaskResult:
        """Addressed result; useful for fan-out or explicit source label."""
        return TaskResult(value, _current_node.get().links[name], label)

    def stop(self) -> None:
        """Stop the executor after current tick."""
        _current_ctrl.get().stop()

    def snapshot(self) -> dict:
        return _current_ctrl.get().snapshot()