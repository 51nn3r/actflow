import asyncio
import random

from actflow import Task, AsyncExecutor
from actflow.control import InputController, OutputController
from actflow.core import Ready, Wait


LAYERS = 3  # network depth
SAMPLES = 6  # samples in the stream


class Feed(Task):
    """Feeds samples into the network tagged with a sequence index for the synchronizer."""

    async def execute(self, inputs, ctx):
        items = next(iter(inputs.values()))
        out = []
        for idx, x in enumerate(items):
            out.append(ctx.to("layer", {"idx": idx, "value": x, "layer": 0}))

        return out


class Layer(Task):
    """Computes one layer with variable delay (distributed worker).
    Routes to itself for the next layer, or to the synchronizer when done."""

    async def execute(self, inputs, ctx):
        item = next(iter(inputs.values()))
        await asyncio.sleep(random.uniform(0.01, 0.05))

        item = dict(item)
        item["value"] = item["value"] * 2 + 1
        item["layer"] += 1

        if item["layer"] < LAYERS:
            return [ctx.to("layer", item)]

        return [ctx.to("done", item)]


class OrderedInput(InputController):
    """Synchronizer input: passes results in strict ascending idx order.
    Holds out-of-order arrivals; the consumed idx travels to the output as a receipt."""

    def __init__(self, labels):
        super().__init__(labels)
        self._slot = self.labels[0]
        self._next = 0
        self._held: dict = {}

    def offer(self, packet):
        item = packet.value
        self._held[item["idx"]] = item

        return self.poll()

    def poll(self):
        return Ready() if self._next in self._held else Wait()

    def collect(self):
        item = self._held.pop(self._next)
        idx = self._next
        self._next += 1

        return {self._slot: item}, idx  # receipt = sequence index


class OrderedOutput(OutputController):
    """Synchronizer output: attaches the idx from the receipt back to the result."""

    def emit(self, results, receipt):
        out = []
        for r in results:
            out.append((r.value, r.node, self.type_label))

        return out


class Collect(Task):
    """Synchronizer terminal: emits values as graph output in original order."""

    def execute(self, inputs, ctx):
        item = next(iter(inputs.values()))

        return [ctx.out((item["idx"], item["value"]))]


def build():
    feed = Feed()()
    layer = Layer()()
    collect = Collect()()
    collect.input = OrderedInput(("done",))
    collect.output = OrderedOutput("Collect")

    feed.link("layer", layer)
    layer.link("layer", layer)  # same node processes every layer in sequence
    layer.link("done", collect)

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
