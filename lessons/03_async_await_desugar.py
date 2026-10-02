"""Урок 03 — async/await как синтаксический сахар над генераторами.

Учит: `async def` возвращает корутину и НЕ запускает тело; её крутят как генератор —
`coro.send(None)` до конца, результат прилетает в `StopIteration.value`.
Ага-момент: `await X` == `yield from X.__await__()`, а event loop — это просто тот, кто send().
"""

from typing import Any, Generator


# =====================================================================
# ЧАСТЬ 1. `async def` создаёт корутину, но НЕ выполняет её тело
# =====================================================================


async def trivial_coro() -> int:
    """Тривиальная корутина без единого await — просто печатает и возвращает число."""
    # ВАЖНО: пока корутину никто не «толкнул», эта строка НЕ исполнится.
    print("  [внутри] тело корутины ПОШЛО (значит, кто-то сделал send)")
    return 42


def part1_coroutine_is_lazy() -> None:
    """Показываем: вызов async-функции не запускает тело, а лишь создаёт объект-корутину."""
    print("=== Часть 1. async def даёт ленивый объект, а не результат ===")

    # Вызов async-функции НЕ печатает «тело пошло». Мы получили лишь объект.
    print("[снаружи] пишу coro = trivial_coro()  (тело ещё спит)")
    coro = trivial_coro()
    print(f"[снаружи] тип объекта: {type(coro).__name__!r} — это корутина, не int")
    print("[снаружи] обрати внимание: строки '[внутри]' пока НЕ было — тело не выполнялось\n")

    # Крутим корутину ровно как генератор из урока 01: первый send обязан быть None (прогрев).
    print("[снаружи] делаю coro.send(None) — впервые толкаю тело корутины:")
    try:
        coro.send(None)
    except StopIteration as done:
        # Корутина без await завершается за один send: тело добегает до return,
        # и Python «выбрасывает» результат внутри StopIteration.value (как в уроке 01).
        print(f"[снаружи] поймал StopIteration, его .value = {done.value!r} — это наш результат")

    print("ВЫВОД: корутина — это приостанавливаемый объект; результат живёт в StopIteration.value.\n")


# =====================================================================
# ЧАСТЬ 2. Простой awaitable: что именно даёт нам `await`
# =====================================================================


class Suspend:
    """Простейший awaitable в стиле урока 04: `await Suspend(tag)` отдаёт наружу tag
    (как «заявку» центральному циклу) и возвращает то, что цикл пришлёт назад через send."""

    def __init__(self, tag: str) -> None:
        self.tag = tag

    def __await__(self) -> Generator[str, Any, Any]:
        # `__await__` ОБЯЗАН вернуть итератор. Проще всего сделать его генератором:
        #   `yield self.tag` — отдаёт метку наружу тому, кто крутит корутину;
        #   answer          — это значение, которое пришлют обратно через .send().
        # Это тот самый двусторонний yield из урока 02, только спрятанный под словом await.
        answer = yield self.tag
        return answer


async def awaiting_coro() -> str:
    """Корутина с двумя await: просит у цикла два ответа и склеивает их."""
    print("  [внутри] await Suspend('A') — отдаю заявку 'A' и замираю")
    a = await Suspend("A")
    print(f"  [внутри] цикл вернул мне для 'A': {a!r}")
    print("  [внутри] await Suspend('B') — отдаю заявку 'B' и замираю")
    b = await Suspend("B")
    print(f"  [внутри] цикл вернул мне для 'B': {b!r}")
    return f"{a}+{b}"


def drive(runnable: Any, replies: list[Any], actor: str) -> Any:
    """Крошечный «драйвер»: крутит корутину/генератор через send и печатает каждый шаг.

    replies — заранее заготовленные ответы, которые уходят ВНУТРЬ на каждую заявку.
    Возвращает финальное значение (то, что окажется в StopIteration.value).
    """
    reply_iter = iter(replies)
    to_send: Any = None  # правило прогрева из урока 02: самый первый send обязан быть None
    step = 0
    while True:
        try:
            # Толкаем объект. Наружу выпадет либо заявка (yield), либо StopIteration (return).
            yielded = runnable.send(to_send)
        except StopIteration as done:
            print(f"  [{actor}] StopIteration — объект завершился, результат = {done.value!r}")
            return done.value

        step += 1
        # Заявка «выпала» наружу — готовим ответ и отправим его назад внутрь следующим send.
        to_send = next(reply_iter)
        print(f"  [{actor}] шаг {step}: наружу пришла заявка {yielded!r}, шлю обратно {to_send!r}")


def part2_await_surfaces_requests() -> None:
    """Гоняем корутину с await руками и видим, как заявки всплывают наружу через send."""
    print("=== Часть 2. Что делает await: отдаёт заявку наружу, ждёт ответ ===")
    print("Крутим корутину сами, БЕЗ asyncio. Мы и есть event loop.\n")

    coro = awaiting_coro()
    result = drive(coro, replies=["ответ-на-A", "ответ-на-B"], actor="цикл")
    print(f"[снаружи] корутина вернула: {result!r}\n")


# =====================================================================
# ЧАСТЬ 3. Главный ага-момент: await X ≈ yield from X.__await__()
# =====================================================================


def desugared_coro() -> Generator[str, Any, str]:
    """РУЧНОЙ перевод awaiting_coro() на язык генераторов — БЕЗ async/await.

    Каждый `await X` заменён на `yield from X.__await__()`. Больше ничего не меняли.
    Это обычный генератор (`def`, а не `async def`), но ведёт себя идентично корутине.
    """
    print("  [внутри] yield from Suspend('A').__await__() — та же заявка 'A'")
    a = yield from Suspend("A").__await__()
    print(f"  [внутри] цикл вернул мне для 'A': {a!r}")
    print("  [внутри] yield from Suspend('B').__await__() — та же заявка 'B'")
    b = yield from Suspend("B").__await__()
    print(f"  [внутри] цикл вернул мне для 'B': {b!r}")
    return f"{a}+{b}"


def part3_desugar_equivalence() -> None:
    """Прогоняем async-версию и ручную generator-версию одним драйвером — вывод совпадает."""
    print("=== Часть 3. await X  ≈  yield from X.__await__() ===")
    print("Компилятор Python переписывает async-тело в генератор по простому правилу:\n")
    print("    async def f():                 def f():            # обычный генератор")
    print("        a = await X       ==>           a = yield from X.__await__()")
    print("        return a                        return a\n")
    print("Докажем это: погоняем ОБЕ версии с одинаковыми ответами и сравним результат.\n")

    same_replies = ["R1", "R2"]

    print("[A] async-версия (awaiting_coro), крутим руками:")
    async_result = drive(awaiting_coro(), replies=list(same_replies), actor="async")

    print("\n[B] ручная generator-версия (desugared_coro), тот же драйвер:")
    gen_result = drive(desugared_coro(), replies=list(same_replies), actor="gen")

    print(f"\n[итог] async-версия  -> {async_result!r}")
    print(f"[итог] generator-версия -> {gen_result!r}")
    print(f"[итог] совпадают? -> {async_result == gen_result}")
    print("ВЫВОД: async/await не добавляет новой семантики — это сахар над yield from.\n")


# =====================================================================
# ЧАСТЬ 4. «Event loop» — это просто тот, кто делает send
# =====================================================================


def part4_loop_is_just_send() -> None:
    """Собираем «мини event loop» в 8 строк и показываем: вся его суть — это send в цикле."""
    print("=== Часть 4. Event loop без магии: цикл вокруг .send() ===")
    print("Настоящий asyncio делает ровно это, только заявки — реальные (сокет готов, таймер истёк).")
    print("Наш цикл на каждую заявку сразу подставляет фиктивный ответ и толкает корутину дальше.\n")

    coro = awaiting_coro()

    # Вот он, весь «движок»: пока корутина жива — тянем из неё заявки и кормим ответами.
    to_send: Any = None
    while True:
        try:
            request = coro.send(to_send)
        except StopIteration as done:
            print(f"[loop] корутина закончилась, финальный результат: {done.value!r}")
            break

        # «Исполнили» заявку request — здесь была бы работа с сокетом/таймером/пулом.
        print(f"[loop] обрабатываю заявку {request!r} и готовлю ответ для send()")
        to_send = f"result({request})"

    print("\nВЫВОД: event loop = while + try/except StopIteration + send().")
    print("Кто держит send() — тот и планировщик. Теперь ты можешь написать свой.\n")


def main() -> None:
    print("Как async/await превращается в знакомые генераторы (уроки 01-02)\n")
    part1_coroutine_is_lazy()
    part2_await_surfaces_requests()
    part3_desugar_equivalence()
    part4_loop_is_just_send()

    print("ИТОГ УРОКА:")
    print("  1) async def f() возвращает корутину и НЕ запускает тело;")
    print("  2) корутину крутят как генератор: coro.send(None) ... до StopIteration.value;")
    print("  3) await X  ==  yield from X.__await__()  — чистый сахар;")
    print("  4) event loop — это просто цикл с .send(). Магии нет.")


if __name__ == "__main__":
    main()


# Следующий урок: 04 — свой awaitable: класс с __await__ (yield self) — прообраз actflow.AwaitRequest
