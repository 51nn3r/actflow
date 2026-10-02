from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from .core import Collected, Delivery, Ready, Verdict, WaitUntil
from .node import NodePort

if TYPE_CHECKING:
    from .node import Node


class Scheduler:
    """State of one graph run: who may run next, and who waits until when."""

    def __init__(self):
        self._parked: dict[Node, float] = {}
        self._busy: set[Node] = set()

    def seed(self, start: Node, value: Any) -> list[tuple[Node, Collected]]:
        """Feed the initial value into the start node's only input."""
        if len(start.inputs) != 1:
            raise ValueError(
                f"{start.name} takes {start.inputs}; a start node needs exactly one input"
            )

        return self.apply(Delivery(NodePort(start, start.inputs[0]), value))

    def apply(self, delivery: Delivery) -> list[tuple[Node, Collected]]:
        """Queue the value on its input; the runs it granted."""
        node = delivery.target.node
        return self._filed(node, node.offer(delivery))

    def wake_due(self) -> list[tuple[Node, Collected]]:
        """The runs granted to parked nodes whose deadline passed."""
        now = time.monotonic()
        ready = []
        for node in [n for n, deadline in self._parked.items() if deadline <= now]:
            del self._parked[node]
            ready += self._filed(node, node.poll())

        return ready

    def timeout(self) -> float | None:
        """Seconds until the nearest deadline, None when nothing is parked."""
        if not self._parked:
            return None

        return max(0.0, min(self._parked.values()) - time.monotonic())

    def _filed(self, node: Node, verdict: Verdict) -> list[tuple[Node, Collected]]:
        """File the verdict; a granted node comes back with its inputs collected.

        Collecting right here keeps the verdict and the dequeue one atomic step,
        so a queued value is never granted twice.
        """
        if isinstance(verdict, Ready):
            if node.isolated and node in self._busy:
                # stays queued; the finish repoll grants it
                return []

            if node.isolated:
                self._busy.add(node)

            self._parked.pop(node, None)
            return [(node, node.collect())]

        if isinstance(verdict, WaitUntil):
            self._parked[node] = verdict.deadline

        return []


class Executor(Scheduler):
    """Waits on the next value of each running node, not on the node finishing."""

    def __init__(self, max_parallel: int = 8):
        super().__init__()
        self.max_parallel = max_parallel
        self._sem = asyncio.Semaphore(max_parallel)
        self._active = False

    def clone(self) -> Executor:
        """A fresh Executor with the same configuration and no run state."""
        return Executor(self.max_parallel)

    async def run(self, start: Node, value: Any = None) -> AsyncIterator[Any]:
        """Yield what leaves the graph, as it leaves.

        To stop at the first result, break out under contextlib.aclosing.
        One Executor serves one run at a time; node-held controller state
        survives runs, so rerun from scratch via node.clear() or node.clone().
        """
        if self._active:
            raise RuntimeError("this Executor is already running; use one Executor per concurrent run")

        self._active = True
        # A fresh run starts from clean scheduler state, whatever ended the last one.
        self._parked.clear()
        self._busy.clear()
        # A semaphore is one-loop state; _active guarantees nobody holds this one now.
        self._sem = asyncio.Semaphore(self.max_parallel)
        ready = self.seed(start, value)
        # task -> the node it advances and the stream it advances
        pending: dict[asyncio.Task, tuple[Node, AsyncIterator[Delivery]]] = {}
        try:
            while ready or pending or self._parked:
                for node, collected in ready:
                    stream = self._run_node(node, collected)
                    pending[asyncio.ensure_future(anext(stream))] = (node, stream)

                if not pending:
                    if not self._parked:
                        break

                    await asyncio.sleep(self.timeout())
                    ready = self.wake_due()
                    continue

                done, _ = await asyncio.wait(
                    pending.keys(),
                    timeout=self.timeout(),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                ready = []
                for task in done:
                    node, stream = pending.pop(task)
                    try:
                        delivery = task.result()
                    except StopAsyncIteration:
                        self._busy.discard(node)
                        # Inputs may have queued up while the node ran.
                        ready += self._filed(node, node.poll())
                        continue

                    # Before the yield, so a consumer breaking there leaves the
                    # stream in pending, where finally closes it.
                    pending[asyncio.ensure_future(anext(stream))] = (node, stream)

                    if delivery.outbound:
                        yield delivery.value
                    else:
                        ready += self.apply(delivery)

                ready += self.wake_due()
        finally:
            for task in pending:
                task.cancel()

            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            # Closing releases the semaphore each stream holds.
            for _, stream in pending.values():
                await stream.aclose()

            self._active = False

    async def _run_node(self, node: Node, collected: Collected) -> AsyncIterator[Delivery]:
        """One node's deliveries, throttled by the parallelism cap."""
        async with self._sem:
            async with aclosing(node.run(collected)) as stream:
                async for delivery in stream:
                    yield delivery
