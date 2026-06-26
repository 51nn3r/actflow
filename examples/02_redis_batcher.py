"""Пример 2. Система выполнения ИИ-задач: батчевание через redis.

Задачи приходят по одной. Узел-групповщик копит их в батч и отправляет на
обработку, когда батч заполнен ИЛИ истёк таймаут (чтобы одиночная задача не
ждала вечно). Обработчик — асинхронное тело, которое кладёт батч в redis и
ждёт ответа; redis замокан in-memory, но контракт async-тела настоящий.

Батч по размеру-и-таймауту — это кастомный контроллер ввода: он отдаёт
WaitUntil(T), пока батч не полон, и Ready, когда полон или время вышло.

Показывает: кастомный контроллер ввода с дедлайном, пробуждение исполнителя
по таймеру, async-тело с ожиданием внешней системы."""

import asyncio
import time

from actflow import Task, Ready, Wait, WaitUntil, AsyncExecutor
from actflow.control import InputController


BATCH_SIZE = 4
BATCH_TIMEOUT = 0.3        # сек: не ждать полный батч дольше этого


class BatchInput(InputController):
    """Контроллер ввода-групповщик: копит пакеты в один слот.
    Готов, когда набрался размер батча или истёк таймаут от первого пакета.
    poll сигналит готовность/дедлайн, collect отдаёт весь батч и чистит."""

    def __init__(self, labels):
        super().__init__(labels)
        self._slot = self.labels[0]
        self._first_at = None

    def offer(self, packet):
        self.queues[self._slot].append(packet)
        if self._first_at is None:
            self._first_at = time.monotonic()

        return self.poll()

    def poll(self):
        q = self.queues[self._slot]
        if not q:
            return Wait()

        full = len(q) >= BATCH_SIZE
        expired = time.monotonic() - self._first_at >= BATCH_TIMEOUT
        if full or expired:
            return Ready()

        return WaitUntil(deadline=self._first_at + BATCH_TIMEOUT)

    def collect(self):
        batch = [p.value for p in self.queues[self._slot]]
        self.queues[self._slot].clear()
        self._first_at = None

        return {self._slot: batch}, len(batch)


class FakeRedis:
    """Мок очереди: обработчик «забирает» батч и возвращает результат."""

    @staticmethod
    async def process(batch):
        await asyncio.sleep(0.05)        # имитация сетевой обработки

        return [x * x for x in batch]    # «модель» возвела в квадрат


class Producer(Task):
    """Поток задач из очереди: выдаёт по одной с интервалом (как поступление
    из redis), остаток гонит на себя по связи 'loop'. Так батч наполняется
    на лету, а не разом."""

    async def execute(self, inputs, ctx):
        items = next(iter(inputs.values()))
        if not items:
            return []

        await asyncio.sleep(0.02)        # задачи приходят растянуто во времени
        head, rest = items[0], items[1:]
        results = [ctx.to("batch", head)]
        if rest:
            results.append(ctx.to("loop", rest))

        return results


class Batcher(Task):
    """Async-тело: получает готовый батч, отправляет в redis, ждёт ответ."""

    async def execute(self, inputs, ctx):
        batch = next(iter(inputs.values()))
        size = len(batch)
        results = await FakeRedis.process(batch)
        print(f"   обработан батч из {size}: {batch} -> {results}")

        return [ctx.out(results)]


def build():
    producer = Producer()()
    batcher = Batcher()()
    batcher.input = BatchInput(("batch",))      # батчевый контроллер ввода
    producer.link("batch", batcher)
    producer.link("loop", producer)             # остаток потока — на себя

    return producer


async def main():
    print(f"батч по размеру {BATCH_SIZE} или таймауту {BATCH_TIMEOUT}с")
    producer = build()
    ex = AsyncExecutor(max_parallel=4)
    # 10 задач: должны лечь в батчи 4 + 4 + хвост 2 (по таймауту)
    result = await ex.run(producer, list(range(1, 11)))

    print("батчей-результатов:", len(result))
    print("состояние:", ex.snapshot())


if __name__ == "__main__":
    asyncio.run(main())
