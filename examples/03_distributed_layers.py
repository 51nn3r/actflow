"""actflow example 03: a layered network of distributed workers.

Every sample loops through the same Layer node once per layer with its own delay,
so samples finish shuffled; the ordered controller pair restores the original order.
"""

import asyncio
import random

from actflow import (
    Downstream, Executor, GraphOutput, OrderedInputController,
    OrderedOutputController, Task,
)

LAYERS = 3
SAMPLES = 6


class Feed(Task):
    """Feeds samples into the network, tagged with a sequence index."""

    def execute(self, items):
        for idx, x in enumerate(items):
            yield Downstream({"idx": idx, "value": x, "layer": 0}, output="layer")


class Layer(Task):
    """Computes one layer with variable delay (distributed worker).
    Routes to itself for the next layer, or to the synchronizer when done."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.finish_order = []

    async def execute(self, item):
        await asyncio.sleep(random.uniform(0.01, 0.05))

        item = dict(item)
        item["value"] = item["value"] * 2 + 1
        item["layer"] += 1

        if item["layer"] < LAYERS:
            return Downstream(item, output="layer")

        self.finish_order.append(item["idx"])
        return Downstream(item, output="done")


class Collect(Task):
    """Synchronizer terminal: emits values as graph output in original order."""

    def execute(self, done):
        return GraphOutput((done["idx"], done["value"]))


def build():
    layer_task = Layer()

    feed = Feed()()
    layer = layer_task()
    collect = Collect(
        input_controller=OrderedInputController(),
        output_controller=OrderedOutputController(),
    )()

    feed["layer"] >> layer
    layer["layer"] >> layer  # every layer runs on this one node
    layer["done"] >> collect

    return feed, layer_task


async def main():
    print(f"network of {LAYERS} layers, {SAMPLES} samples in the stream")
    feed, layer_task = build()
    executor = Executor(max_parallel=4)
    results = [out async for out in executor.run(feed, list(range(SAMPLES)))]

    print("finish order:", layer_task.finish_order)
    print("output (in original order):", results)
    print("ordered:", results == sorted(results))


if __name__ == "__main__":
    asyncio.run(main())
