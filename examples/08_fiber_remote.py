"""actflow example 08: fiber bodies calling a remote service through a gateway.

Feed streams five texts into one Embed node whose ticks run as fibers on a
shared two-thread pool. Each tick awaits self.remote, the gateway files the
request with an in-file request/reply service, and the worker thread is
released while the reply is pending. Five 0.1s replies therefore cost about
0.1s in total: the thread pool, not the executor cap, is the scarce resource,
and it never blocks on latency.
"""

import asyncio
import contextlib
import time

from actflow import (
    Downstream,
    Executor,
    FiberExecutionController,
    GraphOutput,
    RemoteGateway,
    Task,
)

TEXTS = [f"text-{i}" for i in range(5)]
SERVICE_DELAY = 0.1
FIBER_THREADS = 2


class QueueGateway(RemoteGateway):
    """Request/reply over an in-process inbox: submit parks a Future under a
    request id and the background service resolves it with the reply."""

    def __init__(self):
        self._inbox = asyncio.Queue()
        self._pending = {}
        self._next_id = 0
        self.active = 0
        self.peak = 0

    async def submit(self, service, operation, payload):
        request_id = self._next_id
        self._next_id += 1
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        self._inbox.put_nowait((request_id, payload))

        return await future

    async def run_service(self):
        """Drains the inbox, handling every request concurrently."""
        handlers = []
        try:
            while True:
                request_id, payload = await self._inbox.get()
                handlers.append(asyncio.create_task(self._handle(request_id, payload)))
        except asyncio.CancelledError:
            await asyncio.gather(*handlers, return_exceptions=True)
            raise

    async def _handle(self, request_id, payload):
        """One request: simulated service latency, then the reply resolves the Future."""
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(SERVICE_DELAY)
        self.active -= 1
        self._pending.pop(request_id).set_result({"len": len(payload["text"])})


class Feed(Task):
    """Streams each text into the graph as its own value."""

    def execute(self, texts):
        for text in texts:
            yield Downstream(text)


class Embed(Task):
    """Fiber body: sends the text to the remote service and waits without a worker."""

    async def execute(self, text):
        reply = await self.remote("ml", "embed", {"text": text})

        return GraphOutput((text, reply["len"]))


def build(gateway):
    feed = Feed()()
    fiber = FiberExecutionController(threads=FIBER_THREADS, gateway=gateway)
    feed >> Embed(execution_controller=fiber)()

    return feed


async def main():
    print(
        f"{len(TEXTS)} texts -> one fiber Embed node; "
        f"pool = {FIBER_THREADS} threads, reply latency {SERVICE_DELAY}s"
    )
    gateway = QueueGateway()
    service = asyncio.create_task(gateway.run_service())
    start = time.monotonic()
    results = [out async for out in Executor(max_parallel=8).run(build(gateway), TEXTS)]
    elapsed = time.monotonic() - start
    service.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await service

    print("replies:", sorted(results))
    print(f"total {elapsed:.2f}s (serial replies would take {len(TEXTS) * SERVICE_DELAY:.1f}s)")
    print("peak concurrent requests at the service:", gateway.peak)
    assert len(results) == len(TEXTS)
    assert elapsed < len(TEXTS) * SERVICE_DELAY / 2, elapsed


if __name__ == "__main__":
    asyncio.run(main())
