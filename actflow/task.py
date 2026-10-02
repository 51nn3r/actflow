from __future__ import annotations

import inspect
from typing import Any, Callable

from .control import (
    ExecutionControllerInterface,
    InputController,
    InputControllerInterface,
    LocalExecutionController,
    LoopIORequest,
    OffloadRequest,
    OutputController,
    OutputControllerInterface,
    RemoteRequest,
    RoutingPolicyInterface,
    SleepRequest,
)
from .node import Node


class Task:
    """Unit of computation: subclass and override execute() (sync or async)."""

    # Filled once per class at definition time, so a bad body is caught on import.
    inputs: tuple[str, ...] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.inputs = cls._infer_inputs()

    @classmethod
    def _infer_inputs(cls) -> tuple[str, ...]:
        """Input names, taken from execute's parameters. Empty when the body is variadic."""
        inputs = []
        for name, param in inspect.signature(cls.execute).parameters.items():
            if name == "self":
                continue

            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                return ()

            inputs.append(name)

        return tuple(inputs)

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Body of one tick. Override in a subclass."""
        raise NotImplementedError

    def __init__(
        self,
        input_controller: InputControllerInterface | None = None,
        output_controller: OutputControllerInterface | None = None,
        execution_controller: ExecutionControllerInterface | None = None,
        isolated: bool = False,
        policy: RoutingPolicyInterface | None = None,
    ):
        self.input_controller = input_controller or InputController()
        self.output_controller = output_controller or OutputController()
        self.execution_controller = execution_controller or LocalExecutionController()
        self.isolated = isolated
        self.policy = policy

    def __call__(self) -> Node:
        """Build a graph Node. Each node takes its own copy of the controllers."""
        if not self.inputs:
            raise TypeError(
                f"{type(self).__name__}.execute has no named inputs; "
                f"a variadic body has no fixed arity"
            )

        return Node(
            self,
            self.inputs,
            self.input_controller,
            self.output_controller,
            self.execution_controller,
            name=type(self).__name__,
            isolated=self.isolated,
            policy=self.policy,
        )

    def sleep(self, delay: float) -> SleepRequest:
        """Await to pause a fiber body for `delay` seconds without holding a worker."""
        return SleepRequest(delay)

    def loop_io(self, factory: Callable[[], Any]) -> LoopIORequest:
        """Await to run an async factory on the central loop (e.g. aiohttp)."""
        return LoopIORequest(factory)

    def offload(self, fn: Callable[[], Any]) -> OffloadRequest:
        """Await to run a blocking sync callable in the worker pool."""
        return OffloadRequest(fn)

    def remote(self, service: str, operation: str, payload: Any) -> RemoteRequest:
        """Await to send an operation to the remote gateway and get its reply."""
        return RemoteRequest(service, operation, payload)
