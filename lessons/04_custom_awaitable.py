"""Урок 04 — свой awaitable: прямой прообраз actflow.AwaitRequest.

Учит: класс с `def __await__(self): result = yield self; return result` превращает
`await obj` в «отдай заявку драйверу и жди, пока он подставит результат».
Ага-момент: await ЛЮБОГО объекта = yield его наружу event-loop'у, который через .send() кладёт ответ.
"""

from collections.abc import Coroutine
from typing import Any


# ---------------------------------------------------------------------------
# Заявки. Базовый класс — ДОСЛОВНО actflow.AwaitRequest (см. actflow/fiber.py).
# Подклассы просто носят данные и умеют resolve() — «исполниться».
# ---------------------------------------------------------------------------

class AwaitRequest:
    """Базовая «заявка» (request). __await__ отдаёт саму заявку драйверу и ждёт результат.

    Это буквально класс из actflow/fiber.py — тот же самый `yield self`.
    """

    def __await__(self):
        # __await__ ОБЯЗАН вернуть итератор. Генератор — это итератор, поэтому годится.
        # Тело генератора не запускается, пока драйвер не толкнёт его первым .send(None).
        print(f"  [__await__] {type(self).__name__}: делаю `yield self` — заявка выпадает наружу")
        # `yield self` — тот же двусторонний канал из урока 02:
        #   наружу уходит self (заявка), а внутрь вернётся то, что драйвер пришлёт через .send().
        result = yield self
        print(f"  [__await__] {type(self).__name__}: драйвер прислал {result!r} — возвращаю как значение await")
        # return из генератора кладёт значение в StopIteration.value — оно и станет результатом `await`.
        return result

    def resolve(self) -> Any:
        """«Исполнить» заявку и вернуть результат. Драйвер зовёт это, поймав заявку."""
        raise NotImplementedError


class AddRequest(AwaitRequest):
    """Заявка «сложи два числа»."""

    def __init__(self, a: int, b: int) -> None:
        self.a = a
        self.b = b

    def __repr__(self) -> str:
        return f"AddRequest({self.a}, {self.b})"

    def resolve(self) -> int:
        return self.a + self.b


class UpperRequest(AwaitRequest):
    """Заявка «сделай строку заглавной»."""

    def __init__(self, text: str) -> None:
        self.text = text

    def __repr__(self) -> str:
        return f"UpperRequest({self.text!r})"

    def resolve(self) -> str:
        return self.text.upper()


class DivRequest(AwaitRequest):
    """Заявка «подели» — специально умеет упасть (деление на ноль)."""

    def __init__(self, a: int, b: int) -> None:
        self.a = a
        self.b = b

    def __repr__(self) -> str:
        return f"DivRequest({self.a}, {self.b})"

    def resolve(self) -> float:
        return self.a / self.b


# ---------------------------------------------------------------------------
# Крошечный драйвер = мини event loop.
# Ровно то, что делает actflow.run_fiber_step + центральный цикл рантайма.
# ---------------------------------------------------------------------------

def drive(coro: Coroutine, name: str = "coro") -> Any:
    """Гоняет корутину шаг за шагом: .send() -> ловит выпавшую заявку -> resolve() -> .send() назад."""
    to_send: Any = None            # что положим внутрь на следующем .send()
    pending_error: BaseException | None = None  # ошибку «бросим» внутрь через .throw()
    step = 0
    while True:
        step += 1
        try:
            if pending_error is not None:
                # coro.throw поднимает исключение ПРЯМО в точке await (тот же путь, что run_fiber_step)
                request = coro.throw(pending_error)
                pending_error = None
            else:
                # coro.send толкает корутину; наружу выпадет либо заявка, либо StopIteration с результатом
                request = coro.send(to_send)
        except StopIteration as done:
            # Корутина сделала return — её значение лежит в StopIteration.value.
            print(f"[{name}] шаг {step}: корутина завершилась, return = {done.value!r}")
            return done.value

        # Сюда попадаем ТОЛЬКО если наружу выпала заявка (сработал `yield self` внутри __await__).
        print(f"[{name}] шаг {step}: поймал заявку {request!r} — драйвер должен её исполнить")
        if not isinstance(request, AwaitRequest):
            # Тот же guard, что в actflow: тело обязано yield-ить именно AwaitRequest, не что попало.
            raise TypeError(f"из корутины выпало {request!r}, а не AwaitRequest")

        try:
            to_send = request.resolve()  # «исполняем» заявку — как resolve() в реальном рантайме
        except Exception as exc:
            # Заявка не смогла выполниться -> результата нет, вернём внутрь await саму ОШИБКУ.
            print(f"[{name}] шаг {step}: resolve() упал с {exc!r} — верну ошибку внутрь await")
            pending_error = exc
            to_send = None
            continue

        print(f"[{name}] шаг {step}: результат {to_send!r} — отправляю его назад в корутину")


# ---------------------------------------------------------------------------
# Часть 1. Разбираем __await__ РУКАМИ, вообще без async.
# ---------------------------------------------------------------------------

def part1_await_by_hand() -> None:
    """Показываем, что __await__ — это просто генератор, а `yield self` отдаёт заявку наружу."""
    print("\n=== Часть 1. Крутим __await__ руками (без async) ===")
    print("`__await__` — обычный генератор. Возьмём заявку и подёргаем его сами.\n")

    request = AddRequest(2, 3)
    print(f"[рука] request = AddRequest(2, 3) — это просто объект-данные: {request!r}")

    gen = request.__await__()  # это ГЕНЕРАТОР; тело пока не выполнялось
    print(f"[рука] request.__await__() дал генератор: {gen!r}")

    print("[рука] gen.send(None) — запускаем тело; смотрим, ЧТО выпадет наружу:")
    fallen = gen.send(None)
    print(f"[рука] наружу выпало: {fallen!r}")
    print(f"[рука] это ТОТ ЖЕ объект-заявка? fallen is request -> {fallen is request}")

    print("[рука] теперь кладём результат внутрь: gen.send(5)")
    try:
        gen.send(5)
    except StopIteration as stop:
        print(f"[рука] генератор закончился, StopIteration.value = {stop.value!r}")
        print("[рука] ВЫВОД: именно это значение и стало бы результатом выражения `await`.")


# ---------------------------------------------------------------------------
# Часть 2. То же самое, но заявку await-им внутри async def.
# ---------------------------------------------------------------------------

async def one_await_body() -> str:
    """Тело корутины с одним await — просит сложить 10 и 20."""
    print("  [body] дошёл до `answer = await AddRequest(10, 20)`")
    answer = await AddRequest(10, 20)
    print(f"  [body] await вернул {answer!r} — возвращаю итог")
    return f"итог={answer}"


def part2_await_inside_async() -> None:
    """Драйвер вручную (без drive()) показывает: та же заявка выпадает из coro.send()."""
    print("\n=== Часть 2. Та же заявка выпадает уже из async-корутины ===")
    print("await делегирует (yield from) внутрь __await__, поэтому `yield self`")
    print("пробивается сквозь корутину и выпадает наружу прямо из coro.send().\n")

    coro = one_await_body()
    print("[драйвер] coro.send(None) — толкаем корутину первый раз:")
    fallen = coro.send(None)
    print(f"[драйвер] из КОРУТИНЫ выпала заявка: {fallen!r} (тип {type(fallen).__name__})")
    print(f"[драйвер] это AwaitRequest? {isinstance(fallen, AwaitRequest)}")

    print("[драйвер] исполняю заявку и шлю результат назад: coro.send(fallen.resolve())")
    try:
        coro.send(fallen.resolve())
    except StopIteration as stop:
        print(f"[драйвер] корутина завершилась: {stop.value!r}")


# ---------------------------------------------------------------------------
# Часть 3. Крошечный драйвер крутит корутину с НЕСКОЛЬКИМИ await.
# ---------------------------------------------------------------------------

async def pipeline_body() -> str:
    """Тело с двумя разными заявками подряд."""
    total = await AddRequest(2, 3)
    loud = await UpperRequest("hello")
    return f"{total} и {loud!r}"


def part3_tiny_driver() -> None:
    """Отдаём корутину в drive() — он сам исполняет каждую выпавшую заявку."""
    print("\n=== Часть 3. drive() крутит корутину с несколькими await ===")
    print("Каждый await -> заявка выпадает -> драйвер её resolve() -> результат уходит .send() назад.\n")

    result = drive(pipeline_body(), name="pipeline")
    print(f"[итог] {result!r}")


# ---------------------------------------------------------------------------
# Часть 4. Драйвер может вернуть в точку await не результат, а ОШИБКУ.
# ---------------------------------------------------------------------------

async def careful_body() -> int:
    """Тело, которое оборачивает await в try/except — на случай, если заявка упадёт."""
    print("  [body] пробую `await DivRequest(10, 0)` внутри try/except")
    try:
        q = await DivRequest(10, 0)
    except ZeroDivisionError as exc:
        print(f"  [body] await БРОСИЛ {exc!r} — ловлю и подставляю -1")
        q = -1

    return q


def part4_error_into_await() -> None:
    """Показываем: если resolve() упал, драйвер делает coro.throw и await поднимает исключение."""
    print("\n=== Часть 4. Драйвер умеет вернуть в await ОШИБКУ, а не результат ===")
    print("Заявка не смогла выполниться -> драйвер делает coro.throw(exc):")
    print("исключение возникает прямо в точке await (тот же путь, что actflow run_fiber_step).\n")

    result = drive(careful_body(), name="careful")
    print(f"[итог] {result!r}")


# ---------------------------------------------------------------------------
# Часть 5. Что НЕ является валидной заявкой (два способа сломаться).
# ---------------------------------------------------------------------------

async def await_a_plain_int() -> None:
    """Пытаемся await-ить голый int — у него нет __await__."""
    await 42


class RawYielder:
    """Плохой awaitable: его __await__ yield-ит НЕ заявку, а голое число."""

    def __await__(self):
        got = yield 999  # наружу выпадает 999 — это не AwaitRequest
        return got


async def leaky_body() -> Any:
    """Тело, которое await-ит объект с «дырявым» __await__."""
    return await RawYielder()


def part5_what_breaks() -> None:
    """Два способа сломать await: объект без __await__ и __await__, что yield-ит мусор."""
    print("\n=== Часть 5. Что НЕ является валидной заявкой ===")

    print("(а) await объекта без __await__ (int) — Python ругается сам, на самом await:")
    coro = await_a_plain_int()
    try:
        coro.send(None)
    except TypeError as exc:
        print(f"    поймали TypeError: {exc}")

    print("\n(б) __await__ yield-ит не-AwaitRequest — это ловит уже наш драйвер (как actflow):")
    try:
        drive(leaky_body(), name="leaky")
    except TypeError as exc:
        print(f"    поймали TypeError: {exc}")


# ---------------------------------------------------------------------------
# Часть 6. Ага-момент: всё это и есть actflow.AwaitRequest.
# ---------------------------------------------------------------------------

def part6_this_is_actflow() -> None:
    """Явно связываем игрушку с боевым кодом actflow/fiber.py."""
    print("\n=== Часть 6. Ага-момент: это и есть actflow.AwaitRequest ===")
    print("В actflow/fiber.py лежит ДОСЛОВНО тот же класс:\n")
    print("    class AwaitRequest:")
    print("        def __await__(self):")
    print("            result = yield self")
    print("            return result")
    print()
    print("await ЛЮБОГО объекта = «отдай заявку драйверу (yield self) и жди,")
    print("пока он через .send() подставит результат». Центральный event loop сам")
    print("решает, КАК исполнить заявку: SleepRequest -> asyncio.sleep,")
    print("OffloadRequest -> пул потоков, LoopIORequest -> запуск на самом loop.")
    print("Единственная разница с уроком: у нас resolve() синхронный и без аргументов,")
    print("а в actflow — `async def resolve(self, runtime)`. Механика await — та же.")


def main() -> None:
    part1_await_by_hand()
    part2_await_inside_async()
    part3_tiny_driver()
    part4_error_into_await()
    part5_what_breaks()
    part6_this_is_actflow()

    print("\nИТОГ УРОКА: свой awaitable — это класс с `__await__`, который делает `yield self`.")
    print("await = отдать заявку драйверу и ждать, пока он через .send() положит ответ обратно.")


if __name__ == "__main__":
    main()


# Следующий урок: 05 — мини event loop: планировщик, крутящий много корутин разом
