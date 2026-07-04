from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from .core import Packet, Ready, Verdict, WaitUntil

if TYPE_CHECKING:
    from .node import Node


class ExecutorHandle:
    """Executor control handle exposed to tasks via self.stop() / self.snapshot()."""

    def __init__(self, executor: _Base):
        self._executor = executor
        self._stop = False

    @property
    def stopped(self) -> bool:
        return self._stop

    def stop(self) -> None:
        """Request a soft stop: running ticks finish, no new ones start."""
        self._stop = True

    def snapshot(self) -> dict:
        """Delegate to the executor's state snapshot."""
        return self._executor.snapshot()


class ReadyQueue:
    """Ready nodes as a FIFO ordered set: queue order, O(1) dedup."""

    def __init__(self):
        self._nodes: dict = {}

    def __bool__(self) -> bool:
        return bool(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    def add(self, node: Node) -> None:
        """Enqueue the node unless it is already waiting to run."""
        self._nodes.setdefault(node, None)

    def pop(self) -> Node:
        """Remove and return the oldest ready node."""
        node = next(iter(self._nodes))
        del self._nodes[node]
        return node


class WaitSet:
    """Nodes parked on a timed deadline (monotonic seconds)."""

    def __init__(self):
        self._deadlines: dict = {}

    def __len__(self) -> int:
        return len(self._deadlines)

    def set(self, node: Node, deadline: float) -> None:
        """Record or refresh a node's wake deadline."""
        self._deadlines[node] = deadline

    def discard(self, node: Node) -> None:
        """Drop the node from the wait set if present."""
        self._deadlines.pop(node, None)

    def next_deadline(self) -> float | None:
        """Earliest deadline, or None if nothing is waiting."""
        return min(self._deadlines.values()) if self._deadlines else None

    def due(self, now: float) -> list:
        """Pop and return nodes whose deadline has passed."""
        ready = [node for node, dl in self._deadlines.items() if dl <= now]
        for node in ready:
            del self._deadlines[node]

        return ready


class _Base:
    """Shared executor logic: packet delivery, readiness tracking, result dispatch."""

    def __init__(self):
        self.handle = ExecutorHandle(self)
        self.outputs: list = []
        self._ready = ReadyQueue()
        self._waiting = WaitSet()
        self._runs = 0

    def snapshot(self) -> dict:
        """Counters: total runs, collected outputs, ready and waiting nodes."""
        return {"runs": self._runs, "outputs": len(self.outputs),
                "ready": len(self._ready), "waiting": len(self._waiting)}

    def _deliver(self, value: Any, node: Node, label: str) -> None:
        """Offer a packet to a node and record its readiness verdict."""
        self._handle(node, node.offer(Packet(value, label)))

    def _handle(self, node: Node, verdict: Verdict) -> None:
        """Move the node between the ready queue and the waiting set per verdict."""
        if isinstance(verdict, Ready):
            self._ready.add(node)
            self._waiting.discard(node)
        elif isinstance(verdict, WaitUntil):
            self._waiting.set(node, verdict.deadline)

    def _take_ready(self) -> Node:
        """Pop the next ready node (FIFO)."""
        return self._ready.pop()

    def _repoll_due(self) -> None:
        """Re-poll nodes whose deadline has passed."""
        for node in self._waiting.due(time.monotonic()):
            self._handle(node, node.poll())

    def _next_deadline(self) -> float | None:
        """Earliest pending deadline, or None if nothing is waiting."""
        return self._waiting.next_deadline()

    def _collect_results(self, node: Node, results: Any, mark: dict | None) -> None:
        """Deliver a finished node's results to its links or the graph output."""
        for value, target, label in node.dispatch(results, mark):
            if target is None:
                self.outputs.append(value)
            else:
                self._deliver(value, target, label)

    def _after_run(self, node: Node) -> None:
        """Re-check a node's readiness after it ran (leftover queued inputs)."""
        self._handle(node, node.poll())

    def _seed(self, start: Node, value: Any, label: str = "seed") -> None:
        """Inject the initial packet that kicks off the run."""
        self._deliver(value, start, label)


class SyncExecutor(_Base):
    """Runs ready nodes sequentially."""

    def run(self, start: Node, value: Any = None) -> list:
        """Drive the graph to completion, returning collected outputs."""
        self._seed(start, value)
        while not self.handle.stopped:
            if not self._ready:
                deadline = self._next_deadline()
                if deadline is None:
                    break

                time.sleep(max(0.0, deadline - time.monotonic()))
                self._repoll_due()
                continue

            node = self._take_ready()
            results, mark = asyncio.run(node.run(self.handle))
            self._runs += 1
            self._collect_results(node, results, mark)
            self._after_run(node)

        return self.outputs


class AsyncExecutor(_Base):
    """Launches all ready bodies concurrently."""

    def __init__(self, max_parallel: int = 8):
        super().__init__()
        self._sem = asyncio.Semaphore(max_parallel)

    async def run(self, start: Node, value: Any = None) -> list:
        """Drive the graph, running ready bodies concurrently up to max_parallel."""
        self._seed(start, value)
        running: set = set()
        try:
            while True:
                while self._ready and not self.handle.stopped:
                    node = self._take_ready()
                    running.add(asyncio.ensure_future(self._run_node(node)))

                if not running:
                    deadline = self._next_deadline()
                    if deadline is None or self.handle.stopped:
                        break

                    await asyncio.sleep(max(0.0, deadline - time.monotonic()))
                    self._repoll_due()
                    continue

                timeout = self._sleep_timeout()
                done, running = await asyncio.wait(
                    running, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    node, results, mark = task.result()
                    if node is not None:
                        self._runs += 1
                        self._collect_results(node, results, mark)
                        self._after_run(node)

                self._repoll_due()
        finally:
            for task in running:
                task.cancel()

            if running:
                await asyncio.gather(*running, return_exceptions=True)

        return self.outputs

    async def _run_node(self, node: Node) -> tuple:
        """Run one node under the parallelism semaphore; returns (node, results, mark)."""
        async with self._sem:
            if self.handle.stopped:
                return None, None, None

            results, mark = await node.run(self.handle)
            return node, results, mark

    def _sleep_timeout(self) -> float | None:
        """How long to wait for a completion before re-polling deadlines."""
        deadline = self._next_deadline()
        if deadline is None:
            return None

        return max(0.0, deadline - time.monotonic())
