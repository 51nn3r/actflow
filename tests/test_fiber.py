"""The fiber runtime: a body steps in a thread and waits on the loop.

What is checked is not elapsed time but the fact itself: several bodies waiting
at once on a single worker. If waiting held the worker, one body would reach the
barrier and the test would time out.
"""
import asyncio

import pytest
from actflow import AsyncExecutor, Downstream, FiberExecutionController, GraphOutput, Task


async def _collect(stream):
    return [item async for item in stream]


def test_waiting_fiber_frees_its_worker():
    """Three bodies wait at the same time on a pool of one worker."""
    barrier = None

    class Fork(Task):
        def execute(self, value):
            # Three separate nodes, not three values into one: a node runs one
            # execution at a time, so only that one would reach the barrier.
            for i in range(3):
                yield Downstream(i, output=f"o{i}")

    class Waiter(Task):
        async def execute(self, value) -> GraphOutput:
            await self.loop_io(lambda: barrier.wait())
            return GraphOutput(value)

    async def main():
        nonlocal barrier
        barrier = asyncio.Barrier(3)
        fork = Fork()()
        for i in range(3):
            fork[f"o{i}"] >> Waiter(execution_controller=FiberExecutionController())()

        executor = AsyncExecutor(max_parallel=8, fiber_workers=1)
        return await asyncio.wait_for(_collect(executor.run(fork, None)), timeout=10)

    assert sorted(asyncio.run(main())) == [0, 1, 2]


def test_raw_await_in_a_fiber_body_is_rejected():
    """A bare await outside self.sleep/loop_io/offload/remote is an error.

    Which error depends on what is awaited. `asyncio.sleep` fails before the step
    is even inspected, with `RuntimeError: no running event loop`, because the
    body runs in a worker thread where there is no loop at all. Something that
    yields a Future without needing a loop reaches the inspection and fails with
    `TypeError`. The test pins the general rule: it does not pass quietly.
    """

    class Raw(Task):
        async def execute(self, value) -> GraphOutput:
            await asyncio.sleep(0.001)
            return GraphOutput(value)

    node = Raw(execution_controller=FiberExecutionController())()

    async def main():
        return await _collect(AsyncExecutor(fiber_workers=1).run(node, 1))

    with pytest.raises((TypeError, RuntimeError)):
        asyncio.run(main())


def test_fiber_offload_runs_a_blocking_call():
    """`self.offload` moves a blocking call into the pool."""

    class Work(Task):
        async def execute(self, value) -> GraphOutput:
            return GraphOutput(await self.offload(lambda: value * 2))

    node = Work(execution_controller=FiberExecutionController())()

    async def main():
        executor = AsyncExecutor(fiber_workers=2)
        return await asyncio.wait_for(_collect(executor.run(node, 21)), timeout=10)

    assert asyncio.run(main()) == [42]
