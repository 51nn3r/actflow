"""Урок 10 — собираем всё: мини-версия actflow fiber-рантайма в ОДНОМ файле.

Учит: как тело-корутина «шагает» в worker-потоке (один send до yield), а ждёт — на central loop.
Ага-момент: worker занят только быстрыми шагами, а ОЖИДАНИЕ (sleep/сеть) идёт на loop и worker'а
НЕ держит — поэтому N тел, каждое `await SleepRequest(D)`, на пуле из 1 worker уложатся в ≈ D, а не N·D.
Теперь ты можешь переписать это ядро сам: ниже — тот же скелет, что в actflow/actflow/fiber.py.
"""

import asyncio
import time
from collections.abc import Coroutine
from concurrent.futures import ThreadPoolExecutor
from contextvars import Context, ContextVar, copy_context
from dataclasses import dataclass
from typing import Any


# ===========================================================================
# ЧАСТЬ A. Заявки (requests): что тело фибера отдаёт наружу вместо сырого await.
# ===========================================================================

class AwaitRequest:
    """Базовая «заявка». `await request` = ГОЛЫЙ `yield self` наружу тому, кто крутит корутину.

    Голый yield безопасен в ЛЮБОМ потоке (см. урок 08): он не зовёт get_running_loop(),
    не трогает Future — просто отдаёт САМ объект наружу. Значит worker-поток может двигать
    тело через send(), а РЕЗОЛВИТЬ заявку (реально поспать/сходить в сеть) будет central loop.
    """

    def __await__(self):
        # yield self отдаёт заявку наружу; внутрь через .send() вернётся результат от loop'а.
        result = yield self
        return result

    async def resolve(self) -> Any:
        """Central loop делает `await request.resolve()` — тут заявка реально исполняется.
        В настоящем actflow сюда прилетает runtime (чтобы OffloadRequest достал пул) — нам не нужен."""
        raise NotImplementedError


class SleepRequest(AwaitRequest):
    """Таймер: паркует фибер на `delay` секунд, НЕ занимая worker.

    Весь сон происходит внутри resolve() — а resolve() крутит central loop (в СВОЁМ потоке,
    где running loop есть). Поэтому `await asyncio.sleep` тут законен, а в теле фибера — нет.
    """

    def __init__(self, delay: float):
        self.delay = delay

    async def resolve(self) -> None:
        # Мы на central loop — asyncio.sleep находит running loop своего потока и честно паркуется.
        await asyncio.sleep(self.delay)


# ===========================================================================
# ЧАСТЬ B. Исходы одного шага: что вернул один send/throw корутины.
# ===========================================================================

@dataclass(frozen=True)
class Suspended:
    """Тело встало на await и отдало заявку — central loop должен её раз-резолвить."""

    request: AwaitRequest


@dataclass(frozen=True)
class Completed:
    """Тело сделало return — несём финальное значение."""

    value: Any


@dataclass(frozen=True)
class Failed:
    """Тело бросило исключение ИЛИ отдало наружу не-AwaitRequest."""

    error: BaseException


StepOutcome = Suspended | Completed | Failed


# ===========================================================================
# ЧАСТЬ C. Один шаг тела — выполняется В WORKER-ПОТОКЕ.
# ===========================================================================

def run_fiber_step(
    coro: Coroutine,
    context: Context,
    value: Any,
    error: BaseException | None,
) -> StepOutcome:
    """Двигает корутину РОВНО на один шаг (один send/throw) внутри снятого контекста.

    Вызывается из worker-потока через loop.run_in_executor. Два ключевых момента:
    1) шаг = один send() ДО ближайшего yield: корутина выполнит код до `await ...`, отдаст
       заявку — и на этом шаг кончился, worker сразу свободен (он не спит внутри тела);
    2) context.run ОБЯЗАТЕЛЕН: run_in_executor НЕ переносит contextvars в новый поток сам
       (урок 09). context.run(coro.send, ...) — это и есть тот самый ручной перенос контекста.
    """
    try:
        if error is not None:
            # Заявка на прошлом шаге упала — «бросаем» ошибку прямо в точку await тела.
            yielded = context.run(coro.throw, error)
        else:
            # Обычный случай: кладём результат прошлой заявки внутрь и катим тело дальше.
            yielded = context.run(coro.send, value)
    except StopIteration as stop:
        # Тело сделало return — его значение лежит в StopIteration.value.
        return Completed(stop.value)
    except BaseException as exc:
        # Тело бросило что угодно (в т.ч. сырой asyncio.sleep -> RuntimeError, см. урок 08).
        return Failed(exc)

    if isinstance(yielded, AwaitRequest):
        # Наружу выпала корректная заявка — тело припарковано, ждём resolve на loop.
        return Suspended(yielded)

    # Тело отдало наружу мусор (не заявку) — тот же guard, что в боевом actflow.
    return Failed(
        TypeError(
            f"тело фибера отдало {yielded!r}, а не AwaitRequest; "
            f"внутри фибера await-ить можно только заявки (SleepRequest/...), не сырой asyncio"
        )
    )


# ===========================================================================
# ЧАСТЬ D. Драйвер одного фибера — крутится НА CENTRAL LOOP.
# ===========================================================================

async def run_invocation(coro: Coroutine, context: Context, pool: ThreadPoolExecutor) -> Any:
    """Гоняет один фибер до конца: шаг тела в пуле -> резолв заявки на loop -> следующий шаг.

    Разделение труда, ради которого всё и затевалось:
    - run_fiber_step (в worker-потоке) делает быстрый send и СРАЗУ освобождает worker;
    - `await request.resolve()` (на этом же central loop) ждёт таймер/сеть, НЕ занимая worker.
    Пока фибер «спит» на resolve, единственный worker свободен для шагов ДРУГИХ фиберов.
    """
    loop = asyncio.get_running_loop()
    value: Any = None
    error: BaseException | None = None
    while True:
        # Один шаг тела уезжает в worker-поток. Он вернётся быстро: тело либо дойдёт до
        # await и отдаст заявку (Suspended), либо завершится (Completed/Failed).
        outcome = await loop.run_in_executor(pool, run_fiber_step, coro, context, value, error)

        if isinstance(outcome, Completed):
            return outcome.value

        if isinstance(outcome, Failed):
            raise outcome.error

        # Suspended: тело на await. Резолвим заявку ЗДЕСЬ, на central loop — worker уже свободен.
        value, error = None, None
        try:
            value = await outcome.request.resolve()
        except BaseException as exc:
            # Заявка упала — на следующем шаге бросим ошибку внутрь тела (через coro.throw).
            error = exc


# ===========================================================================
# ЧАСТЬ E. Демонстрация. Тела фиберов читают свой id из contextvar (проверка переноса контекста).
# ===========================================================================

BODY_ID: ContextVar[int] = ContextVar("BODY_ID", default=-1)


async def sleepy_body(delay: float) -> str:
    """Правильное тело: ждёт через ЗАЯВКУ. Один await -> два шага (до await и после)."""
    # BODY_ID.get() работает в worker-потоке только потому, что run_fiber_step сделал
    # context.run(coro.send, ...): снятый contextvar доехал до чужого потока (урок 09).
    body_id = BODY_ID.get()
    print(f"    [тело #{body_id}] шаг 1 (worker): дошёл до `await SleepRequest({delay})`, отдаю заявку и освобождаю worker")
    await SleepRequest(delay)
    print(f"    [тело #{body_id}] шаг 2 (worker): loop раз-резолвил заявку и разбудил меня — доработал")
    return f"body#{body_id}-ok"


async def blocking_body(delay: float) -> str:
    """НЕправильное тело для контраста: сон БЛОКИРУЕТ шаг, а значит и единственный worker."""
    body_id = BODY_ID.get()
    print(f"    [тело #{body_id}] сплю time.sleep({delay}) ПРЯМО в шаге — держу worker занятым всё это время")
    time.sleep(delay)  # блокирующий сон внутри send: worker занят, другие шаги ждут очереди
    return f"body#{body_id}-ok"


def spawn(bodies: list, pool: ThreadPoolExecutor) -> list:
    """Оборачивает каждое тело в run_invocation со СВОИМ контекстом (id проставлен в контекст)."""
    invocations = []
    for i, coro in enumerate(bodies):
        context = copy_context()      # урок 09: снимок contextvars для этого фибера
        context.run(BODY_ID.set, i)   # кладём id ВНУТРЬ снятого снимка, а не в общий контекст
        invocations.append(run_invocation(coro, context, pool))

    return invocations


async def demo_concurrent(n: int, delay: float) -> float:
    """N тел `await SleepRequest(D)` на пуле из 1 worker. Ждём — на loop, поэтому итог ≈ D."""
    print(f"=== Демо 1. {n} тел, каждое `await SleepRequest({delay})`, пул из ОДНОГО worker ===")
    print("Ожидание живёт в resolve() на central loop, worker делает только быстрые шаги — тела спят ПАРАЛЛЕЛЬНО.\n")
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        invocations = spawn([sleepy_body(delay) for _ in range(n)], pool)
        start = time.perf_counter()
        results = await asyncio.gather(*invocations)
        elapsed = time.perf_counter() - start
    finally:
        pool.shutdown(wait=False)

    print(f"\n[итог] результаты: {results}")
    print(f"[итог] всего заняло {elapsed:.3f} c при D={delay} и N={n}.")
    print(f"[итог] последовательно было бы N·D = {n * delay:.3f} c, а вышло ≈ D — тела ждали ОДНОВРЕМЕННО.\n")
    return elapsed


async def demo_blocking(n: int, delay: float) -> float:
    """Тот же N и D, но сон БЛОКИРУЕТ единственный worker -> шаги встают в очередь -> ≈ N·D."""
    print(f"=== Демо 2 (контраст). Те же {n} тел, но сон блокирует worker ===")
    print("Здесь time.sleep сидит ВНУТРИ шага и держит единственный worker — тела спят ПО ОЧЕРЕДИ.\n")
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        invocations = spawn([blocking_body(delay) for _ in range(n)], pool)
        start = time.perf_counter()
        await asyncio.gather(*invocations)
        elapsed = time.perf_counter() - start
    finally:
        pool.shutdown(wait=False)

    print(f"\n[итог] всего заняло {elapsed:.3f} c ≈ N·D = {n * delay:.3f} c — блокирующий сон занял worker целиком.")
    print("[итог] ВОТ ПОЧЕМУ ожидание нельзя делать в теле фибера: оно должно уходить в resolve() на loop.\n")
    return elapsed


# ===========================================================================
# ЧАСТЬ F. Три способа, которыми шаг завершается неуспехом (ветки run_fiber_step).
# ===========================================================================

class RawYielder:
    """Плохой awaitable: его __await__ отдаёт наружу голое число, а не заявку."""

    def __await__(self):
        got = yield 999  # наружу выпадает 999 — это НЕ AwaitRequest
        return got


async def bad_yield_body() -> None:
    """Тело, которое отдаёт наружу мусор — run_fiber_step вернёт Failed(TypeError)."""
    await RawYielder()


async def raising_body() -> None:
    """Тело, которое просто бросает — run_fiber_step ловит и вернёт Failed."""
    raise ValueError("тело упало намеренно")


async def raw_sleep_body() -> None:
    """Тело с СЫРЫМ asyncio.sleep в worker-потоке: нет running loop -> RuntimeError (урок 08)."""
    await asyncio.sleep(0.01)


async def demo_failures() -> None:
    """Показываем, что все три «плохих» исхода run_fiber_step аккуратно пробрасываются наружу."""
    print("=== Демо 3. Неуспешные исходы шага (Failed) ===")
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        # (а) тело отдало не-AwaitRequest -> run_fiber_step -> Failed(TypeError).
        try:
            await run_invocation(bad_yield_body(), copy_context(), pool)
        except TypeError as exc:
            print(f"  (а) не-заявка наружу -> TypeError: {exc}")

        # (б) тело бросило обычное исключение -> Failed(ValueError).
        try:
            await run_invocation(raising_body(), copy_context(), pool)
        except ValueError as exc:
            print(f"  (б) тело бросило -> ValueError: {exc!r}")

        # (в) сырой asyncio.sleep в worker-потоке -> нет running loop -> Failed(RuntimeError).
        try:
            await run_invocation(raw_sleep_body(), copy_context(), pool)
        except RuntimeError as exc:
            print(f"  (в) сырой asyncio.sleep в фибере -> RuntimeError: {exc!r}")
    finally:
        pool.shutdown(wait=False)

    print("  ВЫВОД: run_fiber_step превращает StopIteration в Completed, а любой сбой — в Failed,")
    print("  и run_invocation просто re-raise'ит ошибку тому, кто ждал фибер.\n")


def main() -> None:
    print("Урок 10 — мини-actflow: fiber-рантайм целиком в одном файле (стд. библиотека, без actflow)\n")
    n, delay = 5, 0.2

    concurrent = asyncio.run(demo_concurrent(n, delay))
    blocking = asyncio.run(demo_blocking(n, delay))
    asyncio.run(demo_failures())

    print("ИТОГ УРОКА:")
    print(f"  - {n} параллельных ожиданий по {delay}c уложились в {concurrent:.3f}c (≈ D),")
    print(f"    а блокирующий вариант — в {blocking:.3f}c (≈ N·D): разница и есть суть fiber-рантайма;")
    print("  - шаг тела = один send() до yield в worker-потоке, дальше worker свободен;")
    print("  - contextvars переносит в поток context.run (урок 09), сырой asyncio там падает (урок 08);")
    print("  - ожидание уходит в resolve() на central loop и worker'а не держит.")
    print("  Ты только что собрал ядро сам — теперь можешь написать свою версию event loop и править наш.")


if __name__ == "__main__":
    main()


# Настоящая версия — actflow/actflow/fiber.py: тот же AwaitRequest / run_fiber_step / драйвер (там _drive),
# плюс OffloadRequest (блокирующий код в пул) и LoopIORequest (async-фабрика на loop), а также отмена/таймауты
# (CancelledError, asyncio.shield, finally тела). Интеграция — через FiberExecutionController в AsyncExecutor.
# Дальше почитай в fiber.py именно ветки отмены в _drive/_step/_cancel_body — это единственное, чего нет в мини-версии.
