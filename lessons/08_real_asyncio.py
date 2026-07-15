"""Урок 08 — как это делает НАСТОЯЩИЙ asyncio, и почему worker-поток не может обычный await.

Учит: loop крутит корутины (asyncio.run создаёт loop, get_running_loop его достаёт),
Future/Task рождаются у loop, а asyncio.sleep ПЕРВЫМ ДЕЛОМ зовёт get_running_loop().
Ага-момент: примитивы asyncio привязаны к loop СВОЕГО потока — поэтому наш fiber-worker
(поток без loop) не может делать сырой `await asyncio.sleep`, и ему нужен свой AwaitRequest.
"""

import asyncio
import threading
from typing import Any, Generator


# ---------------------------------------------------------------------------
# Часть 1. asyncio.run создаёт loop; loop крутит корутины, рождает Future и Task.
# ---------------------------------------------------------------------------

async def child(name: str) -> str:
    """Обычная корутина. Сама по себе она НИЧЕГО не делает — её должен крутить loop."""
    print(f"    [{name}] я корутина, меня сейчас двигает loop через send()")
    # await sleep(0) — это «уступка»: отдать ход loop'у и тут же попроситься обратно.
    await asyncio.sleep(0)
    print(f"    [{name}] после await sleep(0) loop вернул мне ход — доработала")
    return f"{name}-результат"


async def part1_body() -> None:
    """Тело, которое запустит asyncio.run. Всё внутри крутит ОДИН и тот же loop."""
    # get_running_loop() возвращает loop, который прямо СЕЙЧАС нас исполняет.
    # Он существует, потому что нас запустил asyncio.run(...) — это он создал loop.
    loop = asyncio.get_running_loop()
    print(f"  [part1] get_running_loop() = {type(loop).__name__} — это loop, который нас крутит")

    # Future рождается У loop: loop.create_future(). Это «коробка» из урока 06,
    # но настоящая, привязанная к ЭТОМУ loop. Голым конструктором её не делают.
    fut: asyncio.Future = loop.create_future()
    print(f"  [part1] loop.create_future() -> Future, done() = {fut.done()} (пока пусто)")

    # call_soon просит loop: «на следующем витке дёрни вот эту функцию».
    # Так мы асинхронно положим результат в future и разбудим ждущего.
    loop.call_soon(fut.set_result, "значение-из-loop")
    print("  [part1] loop.call_soon(fut.set_result, ...) — положим результат на след. витке")

    # create_task берёт корутину и ОТДАЁТ ЕЁ loop'у как самостоятельную задачу:
    # loop будет двигать её своими send() параллельно с нами. Корутина без Task/await
    # никем не двигается — это просто объект-генератор.
    task = loop.create_task(child("task-A"))
    print(f"  [part1] loop.create_task(child) -> {type(task).__name__}; loop будет её крутить")

    # await future паркует НАС, пока call_soon не сделает set_result — тогда loop нас будит.
    value = await fut
    print(f"  [part1] await future проснулся -> {value!r}")

    # await task ждёт, пока loop докрутит нашу задачу до return.
    result = await task
    print(f"  [part1] await task -> {result!r} (loop докрутил её до конца)")


def part1_loop_runs_everything() -> None:
    """asyncio.run(main()) создаёт loop, крутит корутину, потом loop закрывает."""
    print("=== Часть 1. asyncio.run создаёт loop; loop крутит корутины, рождает Future/Task ===")
    print("asyncio.run(part1_body()) сейчас создаст event loop, прокрутит тело и закроет loop.\n")

    asyncio.run(part1_body())

    print("\nВЫВОД: loop — это двигатель. get_running_loop() достаёт ЕГО, create_future/create_task")
    print("рождают примитивы, привязанные к НЕМУ. Без loop корутина — просто спящий объект.\n")


# ---------------------------------------------------------------------------
# Часть 2. get_running_loop() в обычном потоке (threading.Thread) кидает RuntimeError.
# ---------------------------------------------------------------------------

def part2_no_loop_in_thread() -> None:
    """Ключевой факт: «running loop» — это состояние ПОТОКА, а не программы."""
    print("=== Часть 2. get_running_loop() в обычном потоке -> RuntimeError ===")
    print("loop бежит в одном потоке. В любом ДРУГОМ потоке running-loop'а нет, и это видно.\n")

    captured: dict[str, str] = {}

    def worker_body() -> None:
        # Мы в обычном threading.Thread. asyncio.run тут никто не звал,
        # значит в ЭТОМ потоке нет running loop — get_running_loop() это докажет.
        print("    [worker-поток] зову asyncio.get_running_loop() без своего loop...")
        try:
            asyncio.get_running_loop()
        except RuntimeError as exc:
            # Вот оно — то самое исключение, которое всё объясняет.
            captured["error"] = str(exc)
            print(f"    [worker-поток] поймал RuntimeError: {exc!r}")

    t = threading.Thread(target=worker_body, name="worker")
    t.start()
    t.join()  # ждём поток детерминированно, без sleep и без гонок

    print(f"\nВЫВОД: get_running_loop() в потоке без loop кидает RuntimeError: {captured['error']!r}.")
    print("«Running loop» — свойство ПОТОКА. Есть loop только там, где крутится asyncio.run.\n")


# ---------------------------------------------------------------------------
# Часть 3. asyncio.sleep ПЕРВЫМ ДЕЛОМ зовёт get_running_loop() (при delay > 0).
# А sleep(0) — особый случай: голый yield, «уступка», loop ему не нужен.
# ---------------------------------------------------------------------------

def part3_sleep_needs_loop() -> None:
    """Разбираем asyncio.sleep РУКАМИ через .send(None), без всякого loop."""
    print("=== Часть 3. asyncio.sleep первым делом зовёт get_running_loop() ===")
    print("Покрутим корутину asyncio.sleep вручную (send), чтобы увидеть, ЧТО она делает внутри.\n")

    # --- sleep(0): особый короткий путь — просто `yield`, никакого loop ---
    print("[рука] asyncio.sleep(0) — особый случай: внутри голый `yield`, это «уступка».")
    zero = asyncio.sleep(0)
    yielded = zero.send(None)  # первый send добегает до внутреннего yield и возвращает его
    print(f"  send(None) -> {yielded!r} (голый yield: ход отдан наружу, loop НЕ трогали)")
    try:
        zero.send(None)  # второй send — корутина досчитала до return
    except StopIteration as stop:
        print(f"  второй send -> StopIteration {stop.value!r}: sleep(0) = чистая уступка хода")

    # --- sleep(delay > 0): ПЕРВОЕ действие тела — events.get_running_loop() ---
    print("\n[рука] asyncio.sleep(0.01) — тут первая же строка тела это get_running_loop().")
    print("  Крутим её send(None) в потоке БЕЗ running loop — она обязана упасть на get_running_loop:")
    real = asyncio.sleep(0.01)
    try:
        real.send(None)  # исполняет тело sleep -> get_running_loop() -> нет loop -> RuntimeError
    except RuntimeError as exc:
        print(f"  send(None) -> RuntimeError: {exc!r}")
        print("  Ход даже не дошёл до таймера — sleep СНАЧАЛА ищет loop и без него не живёт.")
    finally:
        real.close()  # закрываем недокрученную корутину, чтобы не было предупреждения

    print("\nВЫВОД: sleep(delay>0) первым делом делает get_running_loop() — ему нужен loop СВОЕГО")
    print("потока (он планирует таймер call_later и ждёт future). Нет loop в потоке — нет sleep.\n")


# ---------------------------------------------------------------------------
# Часть 4. АГА-МОМЕНТ: worker-поток и почему нужен AwaitRequest вместо сырого await.
# ---------------------------------------------------------------------------

class AwaitRequest:
    """Наш awaitable-протокол: `await AwaitRequest()` делает ГОЛЫЙ `yield self`.

    Ровно как asyncio.sleep(0), он не трогает никакой loop — просто отдаёт САМ объект
    наружу тому, кто крутит корутину. Значит его безопасно двигать в ЛЮБОМ потоке:
    worker сделает send(), получит этот request наружу — и на этом шаг worker'а закончен.
    Резолвить request (реально поспать/сходить в сеть) будет уже центральный loop.
    """

    def __await__(self) -> Generator["AwaitRequest", Any, Any]:
        # yield self: отдаём себя наружу; НИ get_running_loop, НИ Future — потоку всё равно.
        result = yield self
        return result


async def fiber_with_request() -> str:
    """Тело «фибера», написанное ПРАВИЛЬНО: await нашего AwaitRequest, не сырой asyncio."""
    print("      [fiber/request] делаю `await AwaitRequest()` — это голый yield self")
    await AwaitRequest()
    print("      [fiber/request] request раз-резолвили, ход вернулся — доработал")
    return "fiber-ok"


async def fiber_with_raw_sleep() -> str:
    """Тело «фибера», написанное НЕПРАВИЛЬНО: сырой await asyncio.sleep в worker-потоке."""
    print("      [fiber/raw] делаю `await asyncio.sleep(0.01)` — а это лезет за loop потока")
    await asyncio.sleep(0.01)
    return "не-дойдём-сюда"


def run_fiber_step(coro: Any) -> Any:
    """Мини-копия actflow.run_fiber_step: двигаем корутину на ОДИН шаг ВНУТРИ worker-потока.

    Возвращает («yielded», объект) — то, что корутина отдала наружу. Именно так боевой
    run_fiber_step крутит тело фибера в потоке из ThreadPoolExecutor (там coro.send).
    """
    return coro.send(None)


def part4_worker_needs_request() -> None:
    """Ага-момент: worker-поток крутит корутину, но loop'а у него нет — значит raw await нельзя."""
    print("=== Часть 4. Ага-момент: worker-поток без loop -> нужен AwaitRequest ===")
    print("Наш fiber-runtime двигает тело фибера через coro.send() в ОТДЕЛЬНОМ worker-потоке")
    print("(как run_in_executor -> run_fiber_step). У того потока СВОЕГО loop нет. Проверим оба тела.\n")

    # --- Правильное тело: await AwaitRequest() — worker двигает его без проблем ---
    print("[main] Тело с AwaitRequest. Гоняем один шаг в worker-потоке:")
    good: dict[str, Any] = {}

    def run_good() -> None:
        coro = fiber_with_request()
        yielded = run_fiber_step(coro)  # send() внутри потока: голый yield self, loop не нужен
        good["yielded"] = yielded
        good["is_request"] = isinstance(yielded, AwaitRequest)
        coro.close()  # для демо на этом обрываем; в бою loop бы его раз-резолвил и продолжил

    t_good = threading.Thread(target=run_good, name="worker-good")
    t_good.start()
    t_good.join()
    print(f"[main] worker вернул наружу {type(good['yielded']).__name__}; это AwaitRequest? "
          f"{good['is_request']}")
    print("[main] Шаг worker'а прошёл ЧИСТО: он отдал request центральному loop и освободился.\n")

    # --- Неправильное тело: raw await asyncio.sleep — тот же worker-поток падает ---
    print("[main] Тело с сырым `await asyncio.sleep`. Гоняем тот же один шаг в worker-потоке:")
    bad: dict[str, str] = {}

    def run_bad() -> None:
        coro = fiber_with_raw_sleep()
        try:
            run_fiber_step(coro)  # send() -> тело sleep -> get_running_loop() -> RuntimeError
        except RuntimeError as exc:
            bad["error"] = str(exc)
            print(f"      [worker-поток] RuntimeError на sleep: {exc!r}")

    t_bad = threading.Thread(target=run_bad, name="worker-bad")
    t_bad.start()
    t_bad.join()
    print(f"[main] worker с сырым sleep упал: RuntimeError {bad['error']!r}.")
    print("[main] Причина ровно из части 3: asyncio.sleep первым делом зовёт get_running_loop(),")
    print("       а в worker-потоке running-loop'а НЕТ (часть 2). Примитив привязан к чужому loop.\n")

    print("ВОТ ПОЧЕМУ у actflow-фибера свой протокол AwaitRequest:")
    print("  - тело фибера крутится в worker-потоке (там нет loop) и может делать ТОЛЬКО yield self;")
    print("  - `await self.sleep(d)` возвращает SleepRequest (тот же голый yield self) — потоку ок;")
    print("  - worker отдаёт request наружу и освобождается; центральный loop (в СВОЁМ потоке,")
    print("    где loop есть) делает request.resolve() -> реальный `await asyncio.sleep(d)`;")
    print("  - loop будит фибер с результатом, worker крутит его следующий шаг. Сырой await asyncio")
    print("    в теле фибера запрещён — он ушёл бы за loop не в свой поток и упал бы, как выше.\n")


def main() -> None:
    print("Настоящий asyncio: loop крутит корутины, а его примитивы привязаны к loop своего потока\n")
    part1_loop_runs_everything()
    part2_no_loop_in_thread()
    part3_sleep_needs_loop()
    part4_worker_needs_request()

    print("ИТОГ УРОКА:")
    print("  1) asyncio.run(main) СОЗДАЁТ event loop; get_running_loop() достаёт тот, что крутит нас;")
    print("  2) Future и Task рождаются У loop (create_future/create_task) и им же двигаются;")
    print("  3) `await asyncio.sleep(0)` = голый yield, «уступка» ходом; sleep(d>0) СНАЧАЛА зовёт")
    print("     get_running_loop() — без loop СВОЕГО потока он кидает RuntimeError;")
    print("  4) worker-поток (fiber-runtime) loop'а не имеет -> сырой await asyncio там падает ->")
    print("     поэтому тело фибера общается через AwaitRequest (yield self), а resolve делает loop.")


if __name__ == "__main__":
    main()


# Следующий урок: 09 — потоки, GIL, run_in_executor и contextvars: когда нужны потоки и как в них переносится контекст
