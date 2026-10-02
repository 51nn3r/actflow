"""actflow example 02: batching input controller with a size-or-timeout window.

Items drip into the graph one at a time; a custom input controller holds them
until the batch fills or the window closes, then the batch runs as one tick.
"""

import asyncio
import time

from actflow import (
    Collected,
    Downstream,
    Executor,
    GraphOutput,
    InputController,
    Ready,
    Task,
    Wait,
    WaitUntil,
)

BATCH_SIZE = 4
BATCH_TIMEOUT = 0.3
ARRIVAL_DELAY = 0.02


class BatchInputController(InputController):
    """Holds a single-input node's values until `size` arrive or `window` seconds pass.
    Annotated attributes are settings; _first is state, set on the node's copy."""

    size: int
    window: float
    _first = 0.0

    def offer(self, delivery):
        queue = self.queues[self.node.inputs[0]]
        queue.append(delivery.value)
        if len(queue) == 1:
            self._first = time.monotonic()

        return self.poll()

    def poll(self):
        queue = self.queues[self.node.inputs[0]]
        if not queue:
            return Wait()

        if len(queue) >= self.size or time.monotonic() - self._first >= self.window:
            return Ready()

        return WaitUntil(self._first + self.window)

    def collect(self):
        name = self.node.inputs[0]
        queue = self.queues[name]
        batch = list(queue)
        queue.clear()

        return Collected(data={name: batch})


class FakeRedis:
    """In-memory stand-in for an external processing queue."""

    @staticmethod
    async def process(batch):
        await asyncio.sleep(0.05)

        return [x * x for x in batch]


class Producer(Task):
    """Emits the seeded items one at a time, spaced out like live arrivals."""

    async def execute(self, items):
        for item in items:
            await asyncio.sleep(ARRIVAL_DELAY)
            yield Downstream(item, output="batch")


class Batcher(Task):
    """Receives a full batch, sends it to redis, hands the results out of the graph."""

    async def execute(self, batch):
        results = await FakeRedis.process(batch)
        print(f"  processed batch of {len(batch)}: {batch} -> {results}")

        return GraphOutput(results)


def build():
    producer = Producer()()
    controller = BatchInputController(size=BATCH_SIZE, window=BATCH_TIMEOUT)
    batcher = Batcher(input_controller=controller)()
    producer["batch"] >> batcher

    return producer


async def main():
    print(f"batch by size {BATCH_SIZE} or timeout {BATCH_TIMEOUT}s")
    producer = build()
    executor = Executor(max_parallel=4)
    # 10 items: expect batches of 4 + 4 + a tail of 2 released by the timeout
    batches = [batch async for batch in executor.run(producer, list(range(1, 11)))]
    print("batches out:", len(batches))


if __name__ == "__main__":
    asyncio.run(main())
