"""Урок 06 — Future: мост между «готово» и «продолжить корутину».

Учит: свой класс Future (pending→done, set_result, add_done_callback) и планировщик,
где `await future` ПАРКУЕТ корутину, а чей-то set_result будит её через send(значение).
Ага-момент: future — это то, чем asyncio будит корутины; наш run_invocation ждёт
именно future от loop.run_in_executor, а воркер, досчитав шаг, этот future «резолвит».
"""

from collections import deque
from collections.abc import Coroutine, Generator
from typing import Any, Callable


# ---------------------------------------------------------------------------
# Future — «коробка», в которой ПОКА пусто, но однажды появится результат.
# Состояние данных: pending (пусто) -> done (значение положено, назад пути нет).
# Плюс список callback'ов — «когда положат результат, дёрни вот этих».
# ---------------------------------------------------------------------------

class Future:
    """Обещание одного будущего значения. Кто-то его await-ит, кто-то его резолвит."""

    def __init__(self) -> None:
        self._state = "pending"       # "pending" -> "done", только вперёд
        self._result: Any = None      # сюда ляжет значение после set_result
        self._callbacks: list[Callable[["Future"], None]] = []  # кого будить, когда done

    def done(self) -> bool:
        """Готов ли future (результат уже положили)?"""
        return self._state == "done"

    def set_result(self, value: Any) -> None:
        """Положить результат и разбудить всех, кто ждал (дёрнуть их callback'и)."""
        if self._state == "done":
            raise RuntimeError("set_result дважды — future уже done")

        print(f"  [future] set_result({value!r}): pending -> done, бужу {len(self._callbacks)} ждавших")
        self._result = value
        self._state = "done"
        # Забираем callback'и себе и очищаем список: каждый дёрнем РОВНО один раз.
        callbacks = self._callbacks
        self._callbacks = []
        for cb in callbacks:
            cb(self)   # вот тут планировщик вернёт ждавшую корутину в очередь готовых

    def result(self) -> Any:
        """Достать результат. Пока future pending — брать нечего."""
        if self._state != "done":
            raise RuntimeError("result() у ещё не готового future")

        return self._result

    def add_done_callback(self, cb: Callable[["Future"], None]) -> None:
        """Подписаться: «когда future станет done — вызови cb(future)».

        Если future УЖЕ done — зовём сразу, ждать нечего (важный крайний случай).
        """
        if self.done():
            print("  [future] add_done_callback: future уже done — зову callback немедленно")
            cb(self)
            return

        self._callbacks.append(cb)

    def __await__(self) -> Generator["Future", Any, Any]:
        # Вот сердце урока. `await future` разворачивается ровно в этот генератор.
        # Если результат ещё НЕ готов — делаем `yield self`: отдаём САМ future наружу,
        # планировщику. Для него это сигнал: «припаркуй меня и разбуди, когда я стану done».
        if not self.done():
            print("  [future.__await__] future ещё pending -> `yield self` (отдаю себя планировщику)")
            yield self

        # Сюда управление вернётся ТОЛЬКО после set_result — результат уже лежит в коробке.
        value = self.result()
        print(f"  [future.__await__] future done -> возвращаю result() = {value!r}")
        return value


# ---------------------------------------------------------------------------
# Планировщик (мини event loop). Держит очередь ГОТОВЫХ к шагу корутин.
# Корутина, сделавшая `await pending_future`, из очереди ВЫПАДАЕТ (паркуется),
# пока future не станет done — тогда его callback вернёт её обратно в очередь.
# ---------------------------------------------------------------------------

class Loop:
    """Крутит корутины по очереди; future — это механизм парковки и пробуждения."""

    def __init__(self) -> None:
        # Каждый элемент — (корутина, что послать ей через send на её следующем шаге).
        self._ready: deque[tuple[Coroutine, Any]] = deque()

    def schedule(self, coro: Coroutine, value: Any = None) -> None:
        """Поставить корутину в очередь готовых: на следующем витке ей сделают send(value)."""
        self._ready.append((coro, value))

    def run(self) -> None:
        """Главный цикл: пока есть готовые корутины — делаем каждой ровно один шаг."""
        step = 0
        while self._ready:
            step += 1
            coro, value = self._ready.popleft()
            print(f"[loop] шаг {step}: беру готовую {coro.__name__!r}, делаю send({value!r})")
            try:
                yielded = coro.send(value)
            except StopIteration as done:
                # Корутина добежала до return — на этом её жизнь в очереди кончена.
                print(f"[loop] шаг {step}: {coro.__name__!r} завершилась, return = {done.value!r}")
                continue

            # Наружу что-то выпало. По контракту это может быть только pending Future.
            if not isinstance(yielded, Future):
                raise TypeError(f"{coro.__name__!r} сделала yield {yielded!r}, а не Future")

            future = yielded
            print(f"[loop] шаг {step}: {coro.__name__!r} ждёт future -> ПАРКУЮ её и иду к другим")

            # КЛЮЧЕВОЙ МОМЕНТ: корутину НЕ возобновляем. Вместо этого подписываемся —
            # когда future станет done, callback вернёт корутину в очередь с результатом.
            # coro=coro в аргументах фиксирует ТЕКУЩУЮ корутину (иначе замыкание поймает
            # последнюю из цикла — классическая ловушка late binding).
            def wake(fut: Future, coro: Coroutine = coro) -> None:
                print(f"[loop.callback] future готов -> кладу {coro.__name__!r} обратно в очередь готовых")
                self.schedule(coro, fut.result())

            future.add_done_callback(wake)

        print("[loop] очередь готовых пуста — будить больше некого, выходим")


# ---------------------------------------------------------------------------
# Демонстрационные корутины: одна ЖДЁТ future, другая его РЕЗОЛВИТ.
# ---------------------------------------------------------------------------

async def waiter(name: str, future: Future) -> str:
    """Потребитель: висит на `await future`, пока кто-то не положит туда результат."""
    print(f"  [{name}] дошёл до `await future` — сейчас отдам себя планировщику")
    value = await future
    print(f"  [{name}] ПРОСНУЛСЯ! await future вернул {value!r}")
    return f"{name} получил {value!r}"


async def resolver(future: Future, value: Any) -> str:
    """Производитель: делает «работу» и кладёт результат в future, будя ждавших."""
    print(f"  [resolver] я поработал и делаю future.set_result({value!r})")
    future.set_result(value)
    print("  [resolver] результат положен, ждавшие уже возвращены в очередь; завершаюсь")
    return "resolver done"


# ---------------------------------------------------------------------------
# Часть 1. Future голыми руками: pending -> done и как срабатывают callback'и.
# ---------------------------------------------------------------------------

def part1_future_by_hand() -> None:
    """Показываем Future без всякого async: коробка, флаг done и подписки."""
    print("\n=== Часть 1. Future руками: pending -> done и callback'и ===")

    fut = Future()
    print(f"[рука] создал future, done() = {fut.done()} (коробка пуста, pending)")

    def on_done(f: "Future") -> None:
        print(f"    >>> callback увидел: future done, result = {f.result()!r}")

    print("[рука] add_done_callback(on_done): future ещё pending -> callback просто запомнили")
    fut.add_done_callback(on_done)

    print("[рука] теперь set_result(123) — это дёрнет отложенный callback:")
    fut.set_result(123)

    print(f"[рука] future.done() = {fut.done()}, future.result() = {fut.result()!r}")

    print("\n[рука] а если подписаться на УЖЕ готовый future — callback зовётся сразу:")
    fut.add_done_callback(on_done)


# ---------------------------------------------------------------------------
# Часть 2. await уже готового future НЕ паркует корутину.
# ---------------------------------------------------------------------------

def part2_await_ready_future() -> None:
    """Крутим корутину руками: если future done ДО await — yield не случается."""
    print("\n=== Часть 2. await уже готового future НЕ паркует ===")
    print("Если результат положили ДО await, __await__ не делает yield, а сразу отдаёт result.\n")

    fut = Future()
    fut.set_result("готово заранее")
    coro = waiter("сразу", fut)

    print("[рука] делаю coro.send(None) РОВНО один раз:")
    try:
        coro.send(None)
    except StopIteration as done:
        # Ни одной парковки: __await__ увидел done и не сделал yield self.
        print(f"[рука] корутина завершилась за ОДИН send (парковки не было): {done.value!r}")

    print("ВЫВОД: `yield self` происходит ТОЛЬКО для pending future — иначе await мгновенный.")


# ---------------------------------------------------------------------------
# Часть 3. Планировщик: корутина A ждёт future, корутина B его резолвит.
# ---------------------------------------------------------------------------

def part3_wait_and_resolve() -> None:
    """Главная демонстрация: A и C паркуются на future, B будит их одним set_result."""
    print("\n=== Часть 3. Планировщик: A и C ждут future, B его резолвит ===")
    print("Порядок постановки в очередь: A, C, resolver. Следи, как A и C ПАРКУЮТСЯ,")
    print("планировщик переключается на других, а resolver будит обоих сразу.\n")

    loop = Loop()
    shared = Future()
    loop.schedule(waiter("A", shared))
    loop.schedule(waiter("C", shared))
    loop.schedule(resolver(shared, "42"))
    loop.run()

    print("\nВЫВОД: один set_result разбудил ОБОИХ ждавших — add_done_callback хранит список.")
    print("Между парковкой и пробуждением планировщик спокойно крутил другие корутины.")


# ---------------------------------------------------------------------------
# Часть 4. Ага-момент: это и есть asyncio + наш actflow.run_invocation.
# ---------------------------------------------------------------------------

def part4_this_is_asyncio() -> None:
    """Связываем игрушку с боевым кодом: run_invocation ждёт future от run_in_executor."""
    print("\n=== Часть 4. Ага-момент: это и есть asyncio + run_invocation ===")
    print("`await future` = отдать future планировщику и спать, пока set_result не разбудит.")
    print("Так asyncio будит ЛЮБУЮ корутину: под каждым await в конце концов лежит какой-то Future.\n")

    print("В actflow/fiber.py ExecutionRuntime.run_invocation делает по сути:")
    print("    outcome = await loop.run_in_executor(self.pool, run_fiber_step, ...)")
    print()
    print("run_in_executor СРАЗУ возвращает Future и паркует run_invocation (как наш `await`).")
    print("Воркер в отдельном потоке считает один шаг фибера; досчитав, поток потокобезопасно")
    print("делает future.set_result(outcome) — и loop будит run_invocation готовым StepOutcome.")
    print("Тот же мост «готово -> продолжить корутину», что мы собрали руками в этом уроке.")


def main() -> None:
    print("Future — то, чем планировщик будит уснувшую на await корутину.\n")
    part1_future_by_hand()
    part2_await_ready_future()
    part3_wait_and_resolve()
    part4_this_is_asyncio()

    print("\nИТОГ УРОКА:")
    print("  1) Future — коробка pending->done с результатом и списком callback'ов;")
    print("  2) `await pending_future` делает `yield self` — планировщик паркует корутину;")
    print("  3) set_result(v) переводит future в done и дёргает callback'и;")
    print("  4) callback возвращает ждавшую корутину в очередь и будит её через send(v).")


if __name__ == "__main__":
    main()


# Следующий урок: 07 — настоящий неблокирующий IO: selectors + мини-loop, один поток на тысячи сокетов
