from __future__ import annotations

import functools
import inspect
from typing import Any, Callable

from .control import (
    InputController,
    InputControllerInterface,
    OutputController,
    OutputControllerInterface,
    ExecutionControllerInterface,
    LocalExecutionController,
)
from .core import Packet, TaskResult, _current_node, _current_handle
from .node import Node


@functools.lru_cache(maxsize=None)
def _infer_in_labels(cls: type) -> tuple[str, ...]:
    """Input slot names inferred from execute's parameters (minus self/ctx).
    Variadic bodies (*args/**kwargs) have no fixed arity: declare in_labels instead."""
    labels = []
    for name, param in inspect.signature(cls.execute).parameters.items():
        if name in ("self", "ctx"):
            continue

        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise TypeError(
                f"cannot infer input slots from {cls.__name__}.execute: "
                f"variadic parameter '{name}' has no fixed arity; "
                f"set in_labels or supply an input_controller"
            )

        labels.append(name)

    return tuple(labels)


class Task:
    """Unit of computation: subclass and override execute() (sync or async).
    Returns a routing dict {link: value} (None key = graph output) or a list[TaskResult]."""

    in_labels: tuple | None = None
    out_labels: tuple | None = None

    def __init__(
            self,
            *,
            label: str = "",
            input_controller: InputControllerInterface | None = None,
            output_controller: OutputControllerInterface | None = None,
            execution_controller: ExecutionControllerInterface | None = None,
            in_labels: tuple | None = None,
            out_labels: tuple | None = None,
            input_map: dict[str, str] | None = None,
            output_map: dict[str, str] | None = None,
            on_dropped: Callable[[Packet], None] | None = None,
    ):
        self.label = label
        self._input_controller = input_controller
        self._output_controller = output_controller
        self._execution_controller = execution_controller
        self.input_map = input_map
        self.output_map = output_map
        self._on_dropped = on_dropped
        if in_labels is not None:
            self.in_labels = in_labels

        if out_labels is not None:
            self.out_labels = out_labels

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Body of one tick. Override in a subclass."""
        raise NotImplementedError

    def __call__(self) -> Node:
        """Build a graph Node for this task, filling in default controllers."""
        labels = self.in_labels if self.in_labels is not None else _infer_in_labels(type(self))
        if self.input_map is not None and not set(self.input_map.values()) <= set(labels):
            raise ValueError(f"input_map targets must be task slots {labels}, got {self.input_map}")

        source_label = (
                (self.out_labels[0] if self.out_labels else None)
                or self.label
                or type(self).__name__
        )
        input_controller = self._input_controller or InputController(labels)
        output_controller = self._output_controller or OutputController()
        execution_controller = self._execution_controller or LocalExecutionController()
        return Node(self, input_controller, output_controller, execution_controller, name=source_label)

    @property
    def memory(self) -> dict:
        """Per-node dict that persists across ticks."""
        return _current_node.get().memory

    @property
    def links(self) -> dict[str, Node]:
        """This node's outgoing links by socket name."""
        return _current_node.get().links

    def to(self, name: str, value: Any, label: str | None = None) -> TaskResult:
        """Addressed result; useful for fan-out or explicit source label."""
        return TaskResult(value, _current_node.get().links[name], label)

    def stop(self) -> None:
        """Stop the executor after current tick."""
        _current_handle.get().stop()

    def snapshot(self) -> dict:
        """Snapshot of executor state (runs, pending counts)."""
        return _current_handle.get().snapshot()

    def on_dropped(self, packet: Packet) -> None:
        """Called for a packet whose source label was not in input_map. Override or inject."""
        if self._on_dropped is not None:
            self._on_dropped(packet)
