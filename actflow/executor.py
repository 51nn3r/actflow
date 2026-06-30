from __future__ import annotations

import asyncio
import time

from .core import Packet, Ready, TaskResult, WaitUntil


class Controller:
    """Executor control handle exposed to tasks via self.stop() / self.snapshot()."""

    def __init__(self, executor):
        self._executor = executor
        self._stop = False

    @property
    def stopped(self) -> bool:
        return self._stop

    def stop(self):
        self._stop = True

    def snapshot(self) -> dict:
        return self._executor.snapshot()


class _Base:
    """Shared executor logic: packet delivery, readiness tracking, result dispatch."""

    def __init__(self):
        self.control = Controller(self)
        self.outputs: list = []
        self._ready: list = []
        self._ready_set: set = set()
        self._waiting: dict = {}
        self._runs = 0

    def snapshot(self) -> dict:
        return {"runs": self._runs, "outputs": len(self.outputs),
                "ready": len(self._ready), "waiting": len(self._waiting)}

    def _deliver(self, value, node, label):
        self._handle(node, node.input_controller.offer(Packet(value, label)))

    def _handle(self, node, verdict):
        if isinstance(verdict, Ready):
            if node not in self._ready_set:
                self._ready.append(node)
                self._ready_set.add(node)

            self._waiting.pop(node, None)

        elif isinstance(verdict, WaitUntil):
            self._waiting[node] = verdict.deadline

    def _take_ready(self):
        node = self._ready.pop(0)
        self._ready_set.discard(node)
        return node

    def _repoll_due(self):
        now = time.monotonic()
        for node, dl in list(self._waiting.items()):
            if dl <= now:
                self._waiting.pop(node, None)
                self._handle(node, node.input_controller.poll())

    def _next_deadline(self):
        return min(self._waiting.values()) if self._waiting else None

    def _collect_results(self, node, results, mark):
        if results is None:
            return

        if isinstance(results, dict):
            type_label = node.output_controller.type_label
            for link_name, value in results.items():
                if link_name is None:
                    self.outputs.append(value)
                else:
                    self._deliver(value, node.links[link_name], type_label)

            return

        if isinstance(results, TaskResult):
            results = [results]

        for value, target, label in node.output_controller.emit(results, mark):
            if target is None:
                self.outputs.append(value)
                continue

            self._deliver(value, target, label)

    def _after_run(self, node):
        self._handle(node, node.input_controller.poll())

    def _seed(self, start, value, label="seed"):
        self._deliver(value, start, label)


class SyncExecutor(_Base):
    """Runs ready nodes sequentially."""

    def run(self, start, value=None):
        self._seed(start, value)
        while not self.control.stopped:
            if not self._ready:
                deadline = self._next_deadline()
                if deadline is None:
                    break

                time.sleep(max(0.0, deadline - time.monotonic()))
                self._repoll_due()
                continue

            node = self._take_ready()
            results, mark = asyncio.run(node.run(self.control))
            self._runs += 1
            self._collect_results(node, results, mark)
            self._after_run(node)

        return self.outputs


class AsyncExecutor(_Base):
    """Launches all ready bodies concurrently."""

    def __init__(self, max_parallel: int = 8):
        super().__init__()
        self._sem = asyncio.Semaphore(max_parallel)

    async def run(self, start, value=None):
        self._seed(start, value)
        running: set = set()
        while True:
            while self._ready and not self.control.stopped:
                node = self._take_ready()
                running.add(asyncio.ensure_future(self._run_node(node)))

            if not running:
                deadline = self._next_deadline()
                if deadline is None or self.control.stopped:
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

        return self.outputs

    async def _run_node(self, node):
        async with self._sem:
            if self.control.stopped:
                return None, None, None

            results, mark = await node.run(self.control)
            return node, results, mark

    def _sleep_timeout(self):
        deadline = self._next_deadline()
        if deadline is None:
            return None

        return max(0.0, deadline - time.monotonic())