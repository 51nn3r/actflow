from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import aclosing, nullcontext
from contextvars import Context, copy_context
from dataclasses import dataclass, replace
from functools import cached_property
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from .core import (
    Collected,
    Delivery,
    Downstream,
    ExecutionResult,
    GraphOutput,
    NodePort,
    Ready,
    Verdict,
    Wait,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from .node import Node
    from .task import Task


@dataclass(kw_only=True, eq=False)
class Controller:
    """An unbound instance is a prototype: every node copies it.

    Subclasses become dataclasses too, so an annotated attribute is a setting.
    Settings go in fields, per-node state in cached_property.
    """

    node: Node | None = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        dataclass(kw_only=True, eq=False)(cls)

    def __call__(self, node: Node) -> Controller:
        """A working copy for one node."""
        return replace(self, node=node)


class ExecutionControllerInterface(Controller):
    """Runs the task body. Subclass to redirect execution (e.g. to a remote worker).

    run normalizes the four body forms into one async stream of results and
    hands it to drive, the one point where a subclass says where it executes.
    """

    async def run(self, task: Task, data: dict) -> AsyncIterator[ExecutionResult]:
        """Yield the body's results as the drive produces them."""
        async with aclosing(self.drive(self._body_stream(task, data))) as results:
            async for result in results:
                yield result

    async def _body_stream(self, task: Task, data: dict) -> AsyncIterator[ExecutionResult]:
        """Any of the four body forms as one stream; execute runs where drive puts it."""
        body = task.execute(**data)
        if inspect.isasyncgen(body):
            async for result in body:
                try:
                    yield result
                except BaseException:
                    # thrown into our yield: close the body here, so its finally runs
                    await body.aclose()
                    raise

            return

        if inspect.isgenerator(body):
            for result in body:
                try:
                    yield result
                except BaseException:
                    body.close()
                    raise

            return

        if inspect.iscoroutine(body):
            body = await body

        if body is not None:
            yield body

    def drive(self, stream: AsyncIterator[ExecutionResult]) -> AsyncIterator[ExecutionResult]:
        """Execute the normalized stream; the policy a controller exists for."""
        raise NotImplementedError


class AwaitRequest:
    """A suspension point a fiber body yields to its controller.
    __await__ yields the request itself; the drive loop resolves it and resumes the fiber."""

    def __await__(self):
        result = yield self
        return result

    async def resolve(
        self, pool: ThreadPoolExecutor, gateway: RemoteGateway | None, context: Context
    ) -> Any:
        """Awaited by the drive loop to produce the value sent back into the fiber."""
        raise NotImplementedError


class SleepRequest(AwaitRequest):
    """Timer: parks the fiber for `delay` seconds without holding a worker."""

    def __init__(self, delay: float):
        self.delay = delay

    async def resolve(
        self, pool: ThreadPoolExecutor, gateway: RemoteGateway | None, context: Context
    ) -> None:
        await asyncio.sleep(self.delay)


class LoopIORequest(AwaitRequest):
    """Runs an async factory on the central loop (e.g. aiohttp) - the worker stays free."""

    def __init__(self, factory: Callable[[], Awaitable[Any]]):
        self.factory = factory

    async def resolve(
        self, pool: ThreadPoolExecutor, gateway: RemoteGateway | None, context: Context
    ) -> Any:
        # the factory and its coroutine run in a copy of the body's context, as offload does
        ctx = context.copy()
        return await asyncio.get_running_loop().create_task(ctx.run(self.factory), context=ctx)


class OffloadRequest(AwaitRequest):
    """Runs a blocking sync callable in the pool, freeing the current step slot.
    Shares the step pool, so it competes for the same worker slots."""

    def __init__(self, fn: Callable[[], Any]):
        self.fn = fn

    async def resolve(
        self, pool: ThreadPoolExecutor, gateway: RemoteGateway | None, context: Context
    ) -> Any:
        # A copy, as asyncio.to_thread does: fn sees the body's vars, its own sets stay local.
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(pool, context.copy().run, self.fn)


class RemoteRequest(AwaitRequest):
    """Sends an operation to a remote gateway and awaits its reply on the loop.
    The worker stays free while the remote service works."""

    def __init__(self, service: str, operation: str, payload: Any):
        self.service = service
        self.operation = operation
        self.payload = payload

    async def resolve(
        self, pool: ThreadPoolExecutor, gateway: RemoteGateway | None, context: Context
    ) -> Any:
        if gateway is None:
            raise RuntimeError("RemoteRequest requires FiberExecutionController(gateway=...)")

        return await gateway.submit(self.service, self.operation, self.payload)


class RemoteGateway:
    """Request/reply transport for self.remote(...). Implement submit for your queue/service."""

    async def submit(self, service: str, operation: str, payload: Any) -> Any:
        """Deliver the operation and await its reply (e.g. via a request_id -> Future map)."""
        raise NotImplementedError


@dataclass(frozen=True)
class Suspended:
    """Body paused on an await; carries the request the drive loop must resolve."""

    request: AwaitRequest


@dataclass(frozen=True)
class Yielded:
    """Body yielded one result and lives on."""

    value: Any


@dataclass(frozen=True)
class Completed:
    """Body is exhausted."""


@dataclass(frozen=True)
class Failed:
    """Body raised, or yielded something that is not an AwaitRequest."""

    error: BaseException


StepOutcome = Suspended | Yielded | Completed | Failed


class FiberExecutionController(ExecutionControllerInterface):
    """Runs the body as a fiber: steps in a worker pool, awaits resolved on the loop.

    The body may await only self.sleep/loop_io/offload/remote; timeout cancels it.
    One prototype, one pool: replace hands the same pool to every node copy;
    pool= shares it wider. The creator shuts a pool down; an orphan pool is
    joined at interpreter exit.
    """

    threads: int = 1
    timeout: float | None = None
    gateway: RemoteGateway | None = None
    max_inflight: int = 0
    pool: ThreadPoolExecutor | None = None
    inflight: dict | None = None

    def __post_init__(self) -> None:
        if self.pool is None:
            self.pool = ThreadPoolExecutor(max_workers=self.threads)

        if self.inflight is None and self.max_inflight > 0:
            self.inflight = {}

    def _limit(self) -> asyncio.Semaphore | None:
        """The in-flight cap for the current loop; a semaphore is one-loop state."""
        if self.inflight is None:
            return None

        loop = asyncio.get_running_loop()
        if loop not in self.inflight:
            self.inflight[loop] = asyncio.Semaphore(self.max_inflight)

        return self.inflight[loop]

    async def drive(self, stream: AsyncIterator[ExecutionResult]) -> AsyncIterator[ExecutionResult]:
        """Step the stream in worker threads, resolve its awaits on the loop.

        The timeout is a deadline on the whole tick, enforced at the
        controller's own await points: a step in a thread cannot be
        interrupted, so an overrun surfaces when the running step ends.
        On cancellation or timeout the body gets a last throw step, so its
        synchronous finally runs.
        """
        async with self._limit() or nullcontext():
            loop = asyncio.get_running_loop()
            deadline = None if self.timeout is None else loop.time() + self.timeout
            context = copy_context()
            # one fetch coroutine spans many steps: every await up to the next yield
            fetch = stream.asend(None)
            value: Any = None
            error: BaseException | None = None
            try:
                while True:
                    if deadline is not None and loop.time() >= deadline:
                        raise TimeoutError

                    outcome, cancelled = await self._step(loop, fetch, context, value, error)
                    value, error = None, None
                    # first record where the body stands: the farewell throws into fetch
                    match outcome:
                        case Completed() | Failed():
                            fetch = None
                        case Yielded():
                            fetch = stream.asend(None)

                    if cancelled:
                        cause = outcome.error if isinstance(outcome, Failed) else None
                        raise asyncio.CancelledError from cause

                    match outcome:
                        case Completed():
                            return
                        case Failed(exc):
                            raise exc
                        case Yielded(item):
                            yield item
                        case Suspended(request):
                            reply = request.resolve(self.pool, self.gateway, context)
                            remaining = None if deadline is None else deadline - loop.time()
                            try:
                                value = await asyncio.wait_for(reply, remaining)
                            except asyncio.CancelledError:
                                raise
                            except BaseException as exc:
                                # the deadline's own TimeoutError ends the tick; the rest goes into the body
                                if isinstance(exc, TimeoutError) and deadline is not None and loop.time() >= deadline:
                                    raise

                                error = exc
            except (asyncio.CancelledError, TimeoutError, GeneratorExit) as exc:
                if fetch is not None:
                    farewell = exc if isinstance(exc, GeneratorExit) else asyncio.CancelledError()
                    await self._cancel_body(loop, fetch, context, farewell)

                raise

    def _advance(
        self, coro: Coroutine, context: Context, value: Any, error: BaseException | None
    ) -> StepOutcome:
        """Advance one asend/athrow step inside `context`; runs in a worker thread.
        context.run is required - run_in_executor does not carry contextvars into the thread."""
        try:
            if error is not None:
                yielded = context.run(coro.throw, error)
            else:
                yielded = context.run(coro.send, value)
        except StopIteration as stop:
            return Yielded(stop.value)
        except StopAsyncIteration:
            return Completed()
        except BaseException as exc:
            return Failed(exc)

        if isinstance(yielded, AwaitRequest):
            return Suspended(yielded)

        trespass = TypeError(
            f"fiber body yielded {yielded!r}, not an AwaitRequest; "
            f"await only self.sleep/self.loop_io/self.offload/self.remote, not a raw asyncio await"
        )
        # the body is still suspended inside the foreign await: unwind it here,
        # so its finally runs and the stream closes instead of dangling forever
        try:
            context.run(coro.throw, trespass)
            context.run(coro.close)
        except BaseException:
            pass

        return Failed(trespass)

    async def _step(
        self, loop: Any, coro: Coroutine, context: Context, value: Any, error: BaseException | None
    ) -> tuple:
        """Run one step in the pool. A step can't be interrupted mid-flight - coro.send is already
        running in a thread - so on external cancel we wait it out and report cancelled=True."""
        step = loop.run_in_executor(self.pool, self._advance, coro, context, value, error)
        try:
            return await asyncio.shield(step), False
        except asyncio.CancelledError:
            # The shield kept the step alive; really wait it out, surviving repeat cancels.
            while not step.done():
                try:
                    await asyncio.shield(step)
                except asyncio.CancelledError:
                    continue

            return step.result(), True

    async def _cancel_body(self, loop: Any, fetch: Coroutine, context: Context, exc: BaseException) -> None:
        """Throw exc into the body via its fetch, so its (synchronous) finally runs.
        Shielded - the worker thread finishes the throw step even as we are being cancelled."""
        step = loop.run_in_executor(self.pool, self._advance, fetch, context, None, exc)
        try:
            await asyncio.shield(step)
        except asyncio.CancelledError:
            await asyncio.wait({step})


class LocalExecutionController(ExecutionControllerInterface):
    """Default: the event loop itself runs every step of the body."""

    def drive(self, stream: AsyncIterator[ExecutionResult]) -> AsyncIterator[ExecutionResult]:
        """Nothing to redirect: the loop executes the stream as is."""
        return stream


class InputControllerInterface(Controller):
    """Holds what arrived and says when the body may run."""

    @cached_property
    def queues(self) -> dict[str, list]:
        """One list of waiting values per input."""
        return {name: [] for name in self.node.inputs}

    def queue(self, name: str) -> list:
        """The container holding this input's waiting values."""
        return self.queues[name]

    def offer(self, delivery: Delivery) -> Verdict:
        """Take an incoming value and report readiness."""
        raise NotImplementedError

    def poll(self) -> Verdict:
        """Re-report readiness without new data (e.g. after a deadline)."""
        raise NotImplementedError

    def collect(self) -> Collected:
        """Dequeue the inputs the body will receive this tick.

        The executor calls it atomically with the Ready verdict, so a queued
        value is never granted twice.
        """
        raise NotImplementedError


class InputController(InputControllerInterface):
    """FIFO: one queue per input."""

    def offer(self, delivery: Delivery) -> Verdict:
        """Queue the value under the input it was addressed to."""
        self.queues[delivery.target.name].append(delivery.value)
        return self.poll()

    def poll(self) -> Verdict:
        """Ready only when every queue holds at least one value."""
        for queue in self.queues.values():
            if not queue:
                return Wait()

        return Ready()

    def collect(self) -> Collected:
        """Pop one value from each queue into the body's arguments."""
        return Collected(data={name: queue.pop(0) for name, queue in self.queues.items()})


class OrderedInputController(InputController):
    """Releases values in ascending order of their value["idx"]."""

    _next = 0

    @cached_property
    def _held(self) -> dict:
        return {}

    def offer(self, delivery: Delivery) -> Verdict:
        """Stash the value by its index, then report readiness."""
        self._held[delivery.value["idx"]] = delivery.value
        return self.poll()

    def poll(self) -> Verdict:
        """Ready only when the next expected index has arrived."""
        return Ready() if self._next in self._held else Wait()

    def collect(self) -> Collected:
        """Release the next-in-order value; the index rides out in the mark."""
        item = self._held.pop(self._next)
        idx = self._next
        self._next += 1
        return Collected(data={self.node.inputs[0]: item}, metadata={"idx": idx})

    def queue(self, name: str) -> dict:
        return self._held


class RoutingPolicyInterface(Controller):
    """Picks the one consumer of a non-shared value."""

    def choose(self, targets: set[NodePort], value: Any) -> NodePort:
        raise NotImplementedError


class ShortestQueue(RoutingPolicyInterface):
    """Default: the consumer with the shortest target queue."""

    def choose(self, targets: set[NodePort], value: Any) -> NodePort:
        return min(targets, key=lambda port: len(port.node.input_controller.queue(port.name)))


class OutputControllerInterface(Controller):
    """Owns delivery: where a produced value goes, how, and when.

    Called once per value the body produced. Returning an empty list holds the
    value back; async so a controller can wait for its turn before letting it out.
    """

    async def dispatch(self, result: ExecutionResult, metadata: dict | None) -> list[Delivery]:
        """Turn one result into deliveries the executor will apply."""
        raise NotImplementedError

    async def close_tick(self, metadata: dict | None) -> None:
        """The tick produced its last value; called once per tick. Default: nothing."""


class OutputController(OutputControllerInterface):
    """Default: every value leaves at once, addressed by the node's wiring."""

    async def dispatch(self, result: ExecutionResult, metadata: dict | None) -> list[Delivery]:
        """Address the result to the active consumers. Nothing is held back."""
        if isinstance(result, Downstream):
            targets = self.node.targets_for(result.output)
            if not targets:
                # nowhere wired: the value is discarded
                return []

            if result.shared:
                return [Delivery(target, result.value) for target in targets]

            return [Delivery(self.node.policy.choose(targets, result.value), result.value, shared=False)]

        if isinstance(result, GraphOutput):
            return [Delivery(None, result.value, outbound=True)]

        raise TypeError(
            f"{self.node.name} produced {result!r}; a body returns Downstream or GraphOutput"
        )


class OrderedOutputController(OutputController):
    """Releases results in tick order: metadata["idx"] says whose turn it is.

    A dispatch ahead of its turn waits inside the call and holds its executor
    slot while waiting. Safe while ticks are granted in idx order (as
    OrderedInputController does); idx stamped out of order can stall the run.
    """

    _next = 0

    @cached_property
    def _turn(self) -> asyncio.Condition:
        return asyncio.Condition()

    async def dispatch(self, result: ExecutionResult, metadata: dict | None) -> list[Delivery]:
        """Wait for the tick's turn, then address as usual."""
        idx = metadata["idx"]
        async with self._turn:
            await self._turn.wait_for(lambda: self._next == idx)

        return await super().dispatch(result, metadata)

    async def close_tick(self, metadata: dict | None) -> None:
        """Advance the turn. Waits too: a tick may end before its turn came."""
        idx = metadata["idx"]
        async with self._turn:
            await self._turn.wait_for(lambda: self._next == idx)
            self._next = idx + 1
            self._turn.notify_all()
