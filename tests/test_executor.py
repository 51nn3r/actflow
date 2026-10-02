"""Executor behaviour: soft stop, node errors, concurrency.

No wall-clock assertions. Concurrency is proven by meeting on a barrier, so a
serialized run times out rather than merely being slower.
"""
import asyncio

import pytest
from actflow import AsyncExecutor, Downstream, GraphOutput, Task


def test_stop_is_soft(walk):
    """`self.stop()` starts no new ticks; the ones already running finish."""
    ticks = []

    class Tick(Task):
        def execute(self, n) -> Downstream | GraphOutput:
            ticks.append(1)
            if len(ticks) >= 3:
                self.stop()
                return GraphOutput(len(ticks))

            return Downstream(n, output="again")

    node = Tick()()
    node["again"] >> node["n"]

    assert walk(node, None) == [3]
    assert len(ticks) == 3, "no tick may start after the stop"


def test_node_error_does_not_orphan_siblings(walk):
    """A failed node does not leave its siblings in flight."""
    cancelled = []

    class Fork(Task):
        def execute(self, value):
            yield Downstream(None, output="boom")
            yield Downstream(None, output="slow")

    class Boom(Task):
        async def execute(self, value) -> GraphOutput:
            raise RuntimeError("boom")

    class Slow(Task):
        async def execute(self, value) -> GraphOutput:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

            return GraphOutput("never")

    fork = Fork()()
    fork["boom"] >> Boom()()
    fork["slow"] >> Slow()()

    with pytest.raises(RuntimeError, match="boom"):
        walk(fork, None)

    assert cancelled == [True], "the sibling must be cancelled, not left running"


def test_ready_bodies_run_concurrently():
    """Ready nodes run at the same time, not one after another."""
    barrier = None

    class Fork(Task):
        def execute(self, value):
            yield Downstream(None, output="a")
            yield Downstream(None, output="b")

    class Meet(Task):
        async def execute(self, value) -> GraphOutput:
            await barrier.wait()
            return GraphOutput("met")

    async def main():
        nonlocal barrier
        barrier = asyncio.Barrier(2)
        fork = Fork()()
        fork["a"] >> Meet()()
        fork["b"] >> Meet()()
        executor = AsyncExecutor(max_parallel=4)
        return await asyncio.wait_for(
            _collect(executor.run(fork, None)), timeout=5
        )

    assert asyncio.run(main()) == ["met", "met"]


def test_parallelism_is_bounded(walk):
    """No more than `max_parallel` bodies run at once."""
    live, peak = 0, 0

    class Fork(Task):
        def execute(self, value):
            for i in range(6):
                yield Downstream(i, output=f"o{i}")

    class Body(Task):
        async def execute(self, value) -> None:
            nonlocal live, peak
            live += 1
            peak = max(peak, live)
            await asyncio.sleep(0)
            live -= 1

    fork = Fork()()
    for i in range(6):
        fork[f"o{i}"] >> Body()()

    walk(fork, None, max_parallel=2)
    assert peak <= 2, f"{peak} bodies ran at once, the cap is 2"


async def _collect(stream):
    return [item async for item in stream]