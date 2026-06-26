"""Исполнители: доставляют результаты в очереди, запускают готовые узлы,
спят до завершения узла или ближайшего дедлайна, затем переопрашивают.

Синхронный ждёт каждое тело на месте. Асинхронный запускает готовые тела
разом и спит на их завершении; долгие и удалённые тела (корутины) прозрачны."""

from __future__ import annotations

import asyncio
import inspect
import time

from .core import Packet, Ready, WaitUntil
from .node import Ctx


class Controller:
    """Рычаги управления, доступные узлам через ctx.control."""

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
    """Общая логика: доставка пакетов через контроллеры ввода, учёт готовых
    (без дублей) и ждущих по таймеру узлов, раскладка результатов."""

    def __init__(self):
        self.control = Controller(self)
        self.outputs: list = []
        self._ready: list = []                 # узлы, готовые к запуску
        self._ready_set: set = set()           # защита от повторной постановки
        self._waiting: dict = {}               # узел -> дедлайн (батч по времени)
        self._runs = 0

    def snapshot(self) -> dict:
        return {"runs": self._runs, "outputs": len(self.outputs),
                "ready": len(self._ready), "waiting": len(self._waiting)}

    def _deliver(self, value, node, label):
        self._handle(node, node.input.offer(Packet(value, label)))

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
        # переопросить узлы, чей дедлайн наступил: батч-контроллер отдаст неполный
        now = time.monotonic()
        for node, dl in list(self._waiting.items()):
            if dl <= now:
                self._waiting.pop(node, None)
                self._handle(node, node.input.poll())

    def _next_deadline(self):
        return min(self._waiting.values()) if self._waiting else None

    def _collect_results(self, node, receipt, results):
        for value, target, label in node.output.emit(results, receipt):
            if target is None:
                self.outputs.append(value)
                continue

            self._deliver(value, target, label)

    def _seed(self, start, value, label="seed"):
        self._deliver(value, start, label)

    def _start(self, node):
        # собрать входы и подготовить вызов тела
        inputs, receipt = node.input.collect()
        self._runs += 1
        ctx = Ctx(node, self.control)
        results = node.task.execute(inputs, ctx)

        return results, receipt

    def _after_run(self, node):
        # узел мог остаться готовым на остатке очередей — переопросить
        self._handle(node, node.input.poll())


class SyncExecutor(_Base):
    """Готовые узлы — последовательно; каждое тело ждётся на месте."""

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
            results, receipt = self._start(node)
            if inspect.iscoroutine(results):
                results = asyncio.run(results)

            self._collect_results(node, receipt, results or [])
            self._after_run(node)

        return self.outputs


class AsyncExecutor(_Base):
    """Запускает все готовые тела разом, спит на их завершении.
    Дедлайны контроллеров ввода будят исполнитель не позже срока."""

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
                node, receipt, results = task.result()
                if results is not None:
                    self._collect_results(node, receipt, results)
                    self._after_run(node)

            self._repoll_due()

        return self.outputs

    async def _run_node(self, node):
        async with self._sem:
            if self.control.stopped:
                return node, None, None

            results, receipt = self._start(node)
            if inspect.iscoroutine(results):
                results = await results

            return node, receipt, results or []

    def _sleep_timeout(self):
        deadline = self._next_deadline()
        if deadline is None:
            return None

        return max(0.0, deadline - time.monotonic())
