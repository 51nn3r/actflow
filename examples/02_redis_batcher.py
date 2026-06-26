import asyncio
import time

from actflow import Task, Ready, Wait, WaitUntil, AsyncExecutor
from actflow.control import InputController


BATCH_SIZE = 4
BATCH_TIMEOUT = 0.3  # max wait for a full batch, seconds


class BatchInput(InputController):
    """Batching input controller: accumulates packets until size or timeout.
    poll signals readiness or the next deadline; collect returns the full batch."""

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
    """In-memory mock for an external processing queue."""

    @staticmethod
    async def process(batch):
        await asyncio.sleep(0.05)

        return [x * x for x in batch]


class Producer(Task):
    """Emits one item at a time with a delay (simulating redis arrivals),
    routes the remainder back via 'loop' so the batch fills gradually."""

    async def execute(self, inputs, ctx):
        items = next(iter(inputs.values()))
        if not items:
            return []

        await asyncio.sleep(0.02)
        head, rest = items[0], items[1:]
        results = [ctx.to("batch", head)]
        if rest:
            results.append(ctx.to("loop", rest))

        return results


class Batcher(Task):
    """Receives a ready batch, sends it to redis, awaits the result."""

    async def execute(self, inputs, ctx):
        batch = next(iter(inputs.values()))
        size = len(batch)
        results = await FakeRedis.process(batch)
        print(f"   обработан батч из {size}: {batch} -> {results}")

        return [ctx.out(results)]


def build():
    producer = Producer()()
    batcher = Batcher()()
    batcher.input = BatchInput(("batch",))
    producer.link("batch", batcher)
    producer.link("loop", producer)

    return producer


async def main():
    print(f"батч по размеру {BATCH_SIZE} или таймауту {BATCH_TIMEOUT}с")
    producer = build()
    ex = AsyncExecutor(max_parallel=4)
    # 10 items: expected batches 4 + 4 + tail 2 (by timeout)
    result = await ex.run(producer, list(range(1, 11)))

    print("батчей-результатов:", len(result))
    print("состояние:", ex.snapshot())


if __name__ == "__main__":
    asyncio.run(main())
