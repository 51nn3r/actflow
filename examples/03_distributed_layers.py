import asyncio
import random

from actflow import Task, TaskResult, AsyncExecutor, OrderedInputController


LAYERS = 3  # network depth
SAMPLES = 6  # samples in the stream


class Feed(Task):
    """Feeds samples into the network tagged with a sequence index for the synchronizer."""

    async def execute(self, items) -> list[TaskResult]:
        return [
            self.to("layer", {"idx": idx, "value": x, "layer": 0})
            for idx, x in enumerate(items)
        ]


class Layer(Task):
    """Computes one layer with variable delay (distributed worker).
    Routes to itself for the next layer, or to the synchronizer when done."""

    async def execute(self, item) -> dict:
        await asyncio.sleep(random.uniform(0.01, 0.05))

        item = dict(item)
        item["value"] = item["value"] * 2 + 1
        item["layer"] += 1

        if item["layer"] < LAYERS:
            return {"layer": item}

        return {"done": item}


class Collect(Task):
    """Synchronizer terminal: emits values as graph output in original order."""

    def execute(self, done) -> dict:
        return {None: (done["idx"], done["value"])}


def build():
    feed = Feed()()
    layer = Layer()()
    collect = Collect(input_controller=OrderedInputController(("done",)))()

    feed["layer"] >> layer
    layer["layer"] >> layer  # same node processes every layer in sequence
    layer["done"] >> collect

    return feed


async def main():
    print(f"сеть из {LAYERS} слоёв, {SAMPLES} образцов в потоке")
    feed = build()
    ex = AsyncExecutor(max_parallel=4)
    result = await ex.run(feed, list(range(SAMPLES)))

    print("выход (в исходном порядке):", result)
    print("упорядочен:", result == sorted(result))
    print("состояние:", ex.snapshot())


if __name__ == "__main__":
    asyncio.run(main())