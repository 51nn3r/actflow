import asyncio
import time

from actflow import Task, AsyncExecutor, FiberExecutionController, RemoteGateway


class QueueGateway(RemoteGateway):
    """Request/reply по request_id -> Future: submit кладёт запрос в inbox и ждёт свой Future;
    фоновый сервис разбирает inbox и резолвит Future ответом. Модель langio conductor."""

    def __init__(self):
        self._inbox: asyncio.Queue = asyncio.Queue()
        self._pending: dict[int, asyncio.Future] = {}
        self._next_id = 0

    async def submit(self, service: str, operation: str, payload: dict) -> dict:
        loop = asyncio.get_running_loop()
        request_id = self._next_id
        self._next_id += 1
        future = loop.create_future()
        self._pending[request_id] = future
        self._inbox.put_nowait((request_id, service, operation, payload))
        return await future  # worker свободен, пока ждём ответ по этому id

    async def run_service(self) -> None:
        """Фоновый сервис: каждый запрос обрабатывает параллельно (латентность не сериализуется)."""
        handlers: list = []
        try:
            while True:
                request_id, service, operation, payload = await self._inbox.get()
                handlers.append(asyncio.create_task(self._handle(request_id, payload)))
        except asyncio.CancelledError:
            await asyncio.gather(*handlers, return_exceptions=True)
            raise

    async def _handle(self, request_id: int, payload: dict) -> None:
        await asyncio.sleep(0.1)  # латентность сервиса
        self._pending.pop(request_id).set_result({"len": len(payload["text"])})


class Feed(Task):
    """Раскидывает тексты на отдельные fiber-ноды."""

    def execute(self, texts: list) -> list:
        return [self.to(f"e{i}", text) for i, text in enumerate(texts)]


class Embed(Task):
    """Fiber-тело: отправляет текст в удалённый сервис и ждёт ответ, не держа worker."""

    async def execute(self, text: str) -> dict:
        reply = await self.remote("ml", "embed", {"text": text})
        return {None: (text, reply["len"])}


def build(texts: list) -> Feed:
    feed = Feed()()
    for i in range(len(texts)):
        feed[f"e{i}"] >> Embed(execution_controller=FiberExecutionController())()

    return feed


async def main() -> None:
    texts = [f"text-{i}" for i in range(5)]
    print(f"{len(texts)} fiber-нод, каждая шлёт remote-запрос (латентность 0.1с); пул = 2 worker")
    gateway = QueueGateway()
    service = asyncio.create_task(gateway.run_service())
    ex = AsyncExecutor(max_parallel=len(texts) + 1, fiber_workers=2, gateway=gateway)
    start = time.monotonic()
    result = await ex.run(build(texts), texts)
    elapsed = time.monotonic() - start
    service.cancel()
    try:
        await service
    except asyncio.CancelledError:
        pass

    print(f"ответы: {sorted(result)}")
    print(f"суммарно {elapsed:.2f}с (последовательно было бы {len(texts) * 0.1:.1f}с)")
    assert len(result) == len(texts)
    assert elapsed < len(texts) * 0.1, elapsed
    print("удалённые запросы шли параллельно, worker свободен: True")


if __name__ == "__main__":
    asyncio.run(main())
