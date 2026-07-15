from __future__ import annotations

import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from contextvars import Context, copy_context
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from .control import ExecutionControllerInterface
from .core import _current_handle

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from .task import Task


class AwaitRequest:
    """A suspension point a fiber body yields to the runtime.
    __await__ yields the request itself; the central loop resolves it and resumes the fiber."""

    def __await__(self):
        result = yield self
        return result

    async def resolve(self, runtime: ExecutionRuntime) -> Any:
        """Awaited by the central loop to produce the value sent back into the fiber."""
        raise NotImplementedError


class SleepRequest(AwaitRequest):
    """Timer: parks the fiber for `delay` seconds without holding a worker."""

    def __init__(self, delay: float):
        self.delay = delay

    async def resolve(self, runtime: ExecutionRuntime) -> None:
        await asyncio.sleep(self.delay)


class LoopIORequest(AwaitRequest):
    """Runs an async factory on the central loop (e.g. aiohttp) — the worker stays free."""

    def __init__(self, factory: Callable[[], Awaitable[Any]]):
        self.factory = factory

    async def resolve(self, runtime: ExecutionRuntime) -> Any:
        return await self.factory()


class OffloadRequest(AwaitRequest):
    """Runs a blocking sync callable in the pool, freeing the current step slot.
    Stage 1: shares the step pool, so it competes for the same worker slots."""

    def __init__(self, fn: Callable[[], Any]):
        self.fn = fn

    async def resolve(self, runtime: ExecutionRuntime) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(runtime.pool, self.fn)


@dataclass(frozen=True)
class Suspended:
    """Body paused on an await; carries the request the loop must resolve."""

    request: AwaitRequest


@dataclass(frozen=True)
class Completed:
    """Body returned a final value."""

    value: Any


@dataclass(frozen=True)
class Failed:
    """Body raised, or yielded something that is not an AwaitRequest."""

    error: BaseException


StepOutcome = Suspended | Completed | Failed


def run_fiber_step(
    coro: Coroutine,
    context: Context,
    value: Any,
    error: BaseException | None,
) -> StepOutcome:
    """Advance the coroutine one step inside `context`; runs in a worker thread.
    context.run is required — run_in_executor does not carry contextvars into the thread."""
    try:
        if error is not None:
            yielded = context.run(coro.throw, error)
        else:
            yielded = context.run(coro.send, value)
    except StopIteration as stop:
        return Completed(stop.value)
    except BaseException as exc:
        return Failed(exc)

    if isinstance(yielded, AwaitRequest):
        return Suspended(yielded)

    return Failed(
        TypeError(
            f"fiber body yielded {yielded!r}, not an AwaitRequest; "
            f"await only self.sleep/self.loop_io/self.offload, not a raw asyncio await"
        )
    )


async def invoke_task(task: Task, data: dict) -> Any:
    """Wrap the body so a sync or async execute is driven the same way."""
    result = task.execute(**data)
    if inspect.iscoroutine(result):
        result = await result

    return result


class ExecutionRuntime:
    """Owns the worker-thread pool and drives fibers step by step on the central loop."""

    def __init__(self, max_workers: int):
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def shutdown(self) -> None:
        """Release the pool; called when the executor run ends."""
        self.pool.shutdown(wait=False)

    async def run_invocation(self, coro: Coroutine, context: Context) -> Any:
        """Run one body step in the pool, resolve its await on the loop, repeat.
        Steps are awaited one at a time, so a fiber is never in two threads at once."""
        loop = asyncio.get_running_loop()
        value: Any = None
        error: BaseException | None = None
        while True:
            outcome = await loop.run_in_executor(
                self.pool, run_fiber_step, coro, context, value, error
            )
            if isinstance(outcome, Completed):
                return outcome.value

            if isinstance(outcome, Failed):
                raise outcome.error

            value, error = None, None
            try:
                value = await outcome.request.resolve(self)
            except BaseException as exc:
                error = exc


class FiberExecutionController(ExecutionControllerInterface):
    """Opt-in: runs the body as a fiber across worker threads instead of on the loop.
    Requires AsyncExecutor(fiber_workers>0); bodies may await only self.sleep/loop_io/offload."""

    async def run(self, task: Task, data: dict) -> Any:
        handle = _current_handle.get()
        runtime = getattr(handle, "runtime", None) if handle is not None else None
        if runtime is None:
            raise RuntimeError("FiberExecutionController requires AsyncExecutor(fiber_workers>0)")

        context = copy_context()
        return await runtime.run_invocation(invoke_task(task, data), context)
