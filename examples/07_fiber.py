import asyncio
import time

from actflow import Task, AsyncExecutor, FiberExecutionController


def _blocking_double(x: float) -> float:
    time.sleep(0.01)  # stand-in for a blocking sync IO call
    return x * 2


class Feed(Task):
    """Fans the list of delays out to one fiber Job per element."""

    def execute(self, delays: list) -> list:
        return [self.to(f"s{i}", delay) for i, delay in enumerate(delays)]


class Job(Task):
    """A fiber body: every await hands the worker back to the loop.
    sleep = timer on the loop, loop_io = async IO on the loop, offload = blocking call in the pool."""

    async def execute(self, delay: float) -> dict:
        self.memory["runs"] = self.memory.get("runs", 0) + 1
        await self.sleep(delay)
        tag = await self.loop_io(lambda: asyncio.sleep(0.01, result="io"))
        doubled = await self.offload(lambda: _blocking_double(delay))
        return {None: (delay, tag, doubled)}


def build(n: int) -> Feed:
    feed = Feed()()
    for i in range(n):
        feed[f"s{i}"] >> Job(execution_controller=FiberExecutionController())()

    return feed


async def main() -> None:
    n, delay = 8, 0.25
    print(f"{n} fiber-задач, каждая ждёт {delay}с; пул = 1 worker")
    ex = AsyncExecutor(max_parallel=n + 1, fiber_workers=1)
    start = time.monotonic()
    result = await ex.run(build(n), [delay] * n)
    elapsed = time.monotonic() - start

    print(f"суммарно {elapsed:.2f}с (worker висел бы во сне -> {n * delay:.1f}с)")
    print("snapshot:", ex.snapshot())
    assert len(result) == n
    assert elapsed < n * delay / 2, elapsed
    print("worker свободен во время ожидания: True")


if __name__ == "__main__":
    asyncio.run(main())
