from __future__ import annotations

import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from contextvars import Context, copy_context
from dataclasses import dataclass
from enum import Enum, auto
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
    Shares the step pool, so it competes for the same worker slots."""

    def __init__(self, fn: Callable[[], Any]):
        self.fn = fn

    async def resolve(self, runtime: ExecutionRuntime) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(runtime.pool, self.fn)


class RemoteRequest(AwaitRequest):
    """Sends an operation to a remote gateway and awaits its reply on the loop.
    The worker stays free while the remote service works (stage 3)."""

    def __init__(self, service: str, operation: str, payload: Any):
        self.service = service
        self.operation = operation
        self.payload = payload

    async def resolve(self, runtime: ExecutionRuntime) -> Any:
        if runtime.gateway is None:
            raise RuntimeError("RemoteRequest requires AsyncExecutor(..., gateway=...)")

        return await runtime.gateway.submit(self.service, self.operation, self.payload)


class RemoteGateway:
    """Request/reply transport for self.remote(...). Implement submit for your queue/service."""

    async def submit(self, service: str, operation: str, payload: Any) -> Any:
        """Deliver the operation and await its reply (e.g. via a request_id -> Future map)."""
        raise NotImplementedError


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
            f"await only self.sleep/self.loop_io/self.offload/self.remote, not a raw asyncio await"
        )
    )


async def invoke_task(task: Task, data: dict) -> Any:
    """Wrap the body so a sync or async execute is driven the same way."""
    result = task.execute(**data)
    if inspect.iscoroutine(result):
        result = await result

    return result


class FiberState(Enum):
    """Where a fiber is right now; RUNNING = a step in a worker thread, WAITING = on an await."""

    RUNNING = auto()
    WAITING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass(eq=False)
class Fiber:
    """A live body coroutine plus the context its steps run in."""

    coro: Coroutine
    context: Context
    state: FiberState = FiberState.RUNNING


class ExecutionRuntime:
    """Owns the worker-thread pool and drives fibers step by step on the central loop."""

    def __init__(self, max_workers: int, gateway: RemoteGateway | None = None, max_inflight: int = 0):
        self.pool = ThreadPoolExecutor(max_workers=max_workers)
        self.gateway = gateway
        self._inflight = asyncio.Semaphore(max_inflight) if max_inflight > 0 else None
        self._live: set[Fiber] = set()

    def shutdown(self) -> None:
        """Release the pool; called when the executor run ends."""
        self.pool.shutdown(wait=False)

    def snapshot(self) -> dict:
        """Live fiber counts by state, for the executor's snapshot()."""
        running = sum(1 for fiber in self._live if fiber.state is FiberState.RUNNING)
        waiting = sum(1 for fiber in self._live if fiber.state is FiberState.WAITING)
        return {"fibers": len(self._live), "fiber_running": running, "fiber_waiting": waiting}

    async def run_invocation(self, coro: Coroutine, context: Context) -> Any:
        """Drive one fiber to completion, honouring the optional in-flight limit."""
        if self._inflight is None:
            return await self._drive(Fiber(coro, context))

        async with self._inflight:
            return await self._drive(Fiber(coro, context))

    async def _drive(self, fiber: Fiber) -> Any:
        """Loop: run one body step in the pool, resolve its await on the loop, repeat.
        On cancellation, deliver it into the body so its finally runs, then re-raise."""
        loop = asyncio.get_running_loop()
        self._live.add(fiber)
        value: Any = None
        error: BaseException | None = None
        try:
            while True:
                fiber.state = FiberState.RUNNING
                outcome, cancelled = await self._step(loop, fiber, value, error)
                if isinstance(outcome, Completed):
                    fiber.state = FiberState.COMPLETED
                    return outcome.value

                if isinstance(outcome, Failed):
                    fiber.state = FiberState.FAILED
                    raise outcome.error

                if cancelled:
                    raise asyncio.CancelledError

                fiber.state = FiberState.WAITING
                value, error = None, None
                try:
                    value = await outcome.request.resolve(self)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    error = exc
        except asyncio.CancelledError:
            fiber.state = FiberState.CANCELLED
            await self._cancel_body(loop, fiber)
            raise
        finally:
            self._live.discard(fiber)

    async def _step(self, loop: Any, fiber: Fiber, value: Any, error: BaseException | None) -> tuple:
        """Run one step in the pool. A step can't be interrupted mid-flight — coro.send is already
        running in a thread — so on external cancel we wait it out and report cancelled=True."""
        step = loop.run_in_executor(self.pool, run_fiber_step, fiber.coro, fiber.context, value, error)
        try:
            return await step, False
        except asyncio.CancelledError:
            await asyncio.wait({step})
            return step.result(), True

    async def _cancel_body(self, loop: Any, fiber: Fiber) -> None:
        """Throw CancelledError into the body so its (synchronous) finally runs.
        Shielded — the worker thread finishes the throw step even as we are being cancelled."""
        step = loop.run_in_executor(
            self.pool, run_fiber_step, fiber.coro, fiber.context, None, asyncio.CancelledError()
        )
        try:
            await asyncio.shield(step)
        except asyncio.CancelledError:
            await asyncio.wait({step})


class FiberExecutionController(ExecutionControllerInterface):
    """Opt-in: runs the body as a fiber across worker threads instead of on the loop.
    Requires AsyncExecutor(fiber_workers>0); bodies may await only self.sleep/loop_io/offload/remote.
    timeout (seconds) cancels the fiber and runs its finally if the body overruns."""

    def __init__(self, timeout: float | None = None):
        self.timeout = timeout

    async def run(self, task: Task, data: dict) -> Any:
        handle = _current_handle.get()
        runtime = getattr(handle, "runtime", None) if handle is not None else None
        if runtime is None:
            raise RuntimeError("FiberExecutionController requires AsyncExecutor(fiber_workers>0)")

        context = copy_context()
        invocation = runtime.run_invocation(invoke_task(task, data), context)
        if self.timeout is None:
            return await invocation

        return await asyncio.wait_for(invocation, self.timeout)
