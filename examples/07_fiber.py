"""actflow example 07: fiber bodies that free their worker thread on every await.

Eight jobs tick one node whose FiberExecutionController owns a single worker
thread. Each body awaits self.sleep (a loop timer), self.loop_io (async IO on
the loop) and self.offload (a blocking call in the pool); while a body waits,
the worker steps other fibers, so the batch takes about one delay, not eight.
The body is an async generator: it streams a progress result mid-tick, so the
doubled value can travel the graph before the tick ends.
"""

import asyncio
import time

from actflow import Downstream, Executor, FiberExecutionController, GraphOutput, Task

JOBS = 8
DELAY = 0.25
IO_DELAY = 0.01


def blocking_double(x):
    """Stand-in for a blocking sync IO call; self.offload runs it in the pool."""
    time.sleep(IO_DELAY)
    return x * 2


class Feed(Task):
    """Puts one delay per job on the wire; each value becomes one fiber tick."""

    def execute(self, delays):
        for delay in delays:
            yield Downstream(delay)


class Job(Task):
    """A fiber body: every await frees the worker, every yield streams a result."""

    async def execute(self, delay):
        await self.sleep(delay)
        tag = await self.loop_io(lambda: asyncio.sleep(IO_DELAY, result="io"))
        doubled = await self.offload(lambda: blocking_double(delay))
        yield GraphOutput(("progress", delay, tag))
        yield GraphOutput(("done", delay, doubled))


def build():
    feed = Feed()()
    job = Job(execution_controller=FiberExecutionController(threads=1))()
    feed >> job

    return feed


async def main():
    print(f"{JOBS} fiber jobs, each sleeping {DELAY}s; pool = 1 worker thread")
    executor = Executor(max_parallel=JOBS + 1)
    start = time.monotonic()
    results = [out async for out in executor.run(build(), [DELAY] * JOBS)]
    elapsed = time.monotonic() - start

    print(f"total {elapsed:.2f}s (a worker stuck in the sleeps would need {JOBS * DELAY:.1f}s)")
    assert len(results) == JOBS * 2, results
    assert elapsed < JOBS * DELAY / 2, elapsed
    done = [r for r in results if r[0] == "done"]
    print("each job streamed a progress result, then:", done[0])
    print("worker free while bodies wait: True")


if __name__ == "__main__":
    asyncio.run(main())
