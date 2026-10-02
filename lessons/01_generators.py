"""Урок 01 — генератор как приостанавливаемая функция, фундамент корутин.

Учит: как `def ... yield` даёт функцию, которая умеет замереть на середине,
сохранить локальные переменные и продолжить с того же места (next/send/throw/close).
Ага-момент: генератор = функция с ПАУЗАМИ, ровно на этом стоят async-корутины.
"""

from typing import Generator


# =====================================================================
# ЧАСТЬ 1. Вызов функции-генератора НЕ выполняет её тело
# =====================================================================


def counting_machine(start: int) -> Generator[int, str, str]:
    """Считалка, которую мы будем гонять руками, шаг за шагом.

    Аннотация Generator[int, str, str] читается так: наружу через yield
    отдаём int, внутрь через send() принимаем str, а в конце return-им str.
    """
    # ВАЖНО: пока никто не сделал next()/send(), эти строки НЕ исполняются.
    print("  [внутри] тело генератора СТАРТОВАЛО, start =", start)
    current = start

    # Первая пауза. yield делает две вещи сразу:
    #   1) отдаёт значение current наружу (тому, кто вызвал next/send);
    #   2) ЗАМОРАЖИВАЕТ функцию прямо здесь — локальная current остаётся жить.
    print("  [внутри] мы сейчас ЗДЕСЬ: отдаю", current, "и замираю на yield #1")
    received = yield current

    # Сюда управление вернётся только при СЛЕДУЮЩЕМ next()/send().
    print("  [внутри] проснулись после yield #1 — продолжаем ПОСЛЕ него")
    print("  [внутри] локальная current пережила паузу, current =", current)
    print("  [внутри] через send() нам передали:", repr(received))
    current += 1

    print("  [внутри] мы сейчас ЗДЕСЬ: отдаю", current, "и замираю на yield #2")
    received = yield current

    print("  [внутри] проснулись после yield #2, передали:", repr(received))
    print("  [внутри] тело кончилось — сейчас сделаю return")

    # return в генераторе НЕ отдаёт значение как обычная функция:
    # оно уедет внутрь исключения StopIteration (см. часть 3).
    return "машинка остановлена"


def part1_creation_and_stepping() -> None:
    print("=" * 62)
    print("ЧАСТЬ 1. Создание генератора и ручной пошаговый прогон")
    print("=" * 62)

    print("[main] вызываю counting_machine(10) ...")
    gen = counting_machine(10)

    # Обрати внимание: НИ ОДНОГО '[внутри]' ещё не напечатано.
    # Вызов вернул объект-генератор, а тело функции даже не начиналось.
    print("[main] получил объект:", gen)
    print("[main] тип:", type(gen).__name__, "— тело ЕЩЁ не выполнялось")
    print()

    # next(gen) == gen.send(None): запускает тело до первого yield.
    print("[main] делаю next() #1 — запускаю до первого yield")
    value = next(gen)
    print("[main] next() вернул:", value, "(это то, что ушло через yield)")
    print()

    # Между вызовами генератор ЗАМОРОЖЕН. Здесь исполняется main, не он.
    print("[main] генератор сейчас заморожен; я, main, свободно делаю свои дела")
    print()

    # send('привет') будит генератор: 'привет' становится значением
    # выражения (received = yield current) внутри него.
    print("[main] делаю send('привет') — будим и передаём значение внутрь")
    value = gen.send("привет")
    print("[main] send() вернул следующий yield:", value)
    print()

    print("[main] делаю send('пока') — толкаем к концу функции")
    try:
        gen.send("пока")

    except StopIteration as stop:
        # Когда тело дошло до конца/return — вылетает StopIteration.
        print("[main] генератор закончился -> StopIteration")
        print("[main] а return-значение лежит в .value:", repr(stop.value))

    print()


# =====================================================================
# ЧАСТЬ 2. Локальные переменные реально живут между паузами
# =====================================================================


def accumulator() -> Generator[int, int, None]:
    """Копит сумму. Между yield'ами total не обнуляется — состояние живёт."""
    total = 0
    while True:
        # Отдаём накопленное и ждём следующее слагаемое через send().
        addition = yield total
        total += addition


def part2_state_survives() -> None:
    print("=" * 62)
    print("ЧАСТЬ 2. Состояние (локальные переменные) переживает паузу")
    print("=" * 62)

    acc = accumulator()
    next(acc)  # раскрутка до первого yield (total ещё 0)
    print("[main] стартовая сумма после раскрутки:", 0)

    for n in (5, 10, 100):
        running = acc.send(n)
        print(f"[main] отправил {n:>3} -> генератор помнит и вернул сумму {running}")

    print("[main] вывод: одна и та же total пережила три паузы — это память корутины")
    print()


# =====================================================================
# ЧАСТЬ 3. StopIteration и return X, положенный в .value
# =====================================================================


def three_then_done() -> Generator[str, None, str]:
    """Отдаёт три реплики и заканчивается с return-значением."""
    yield "раз"
    yield "два"
    yield "три"
    return "это ушло в StopIteration.value"


def part3_stopiteration() -> None:
    print("=" * 62)
    print("ЧАСТЬ 3. Конец генератора: StopIteration и return -> .value")
    print("=" * 62)

    gen = three_then_done()

    # Крутим вручную, ловим момент завершения.
    while True:
        try:
            item = next(gen)
            print("[main] очередной yield:", item)

        except StopIteration as stop:
            # StopIteration — это НЕ ошибка, а штатный сигнал «я закончил».
            # Именно его прячет обычный for, когда цикл сам останавливается.
            print("[main] поймал StopIteration — генератор исчерпан")
            print("[main] return-значение приехало в .value:", repr(stop.value))
            break

    print("[main] кстати, обычный `for x in gen` ловит этот StopIteration за тебя")
    print()


# =====================================================================
# ЧАСТЬ 4. .throw() — исключение влетает В точку yield
# =====================================================================


def resilient_worker() -> Generator[str, None, None]:
    """Умеет пережить брошенное в него исключение и продолжить работу."""
    while True:
        try:
            # Когда снаружи вызовут gen.throw(ValueError(...)), исключение
            # «материализуется» ровно ЗДЕСЬ, на этой строке yield.
            command = yield "жду команду"
            print("  [внутри] получил команду:", repr(command))

        except ValueError as err:
            # Ловим влетевшее исключение прямо внутри генератора.
            print("  [внутри] ВЛЕТЕЛО исключение в точке yield:", err)
            print("  [внутри] обработал и продолжаю жить дальше")


def part4_throw() -> None:
    print("=" * 62)
    print("ЧАСТЬ 4. .throw(): исключение влетает прямо в точку yield")
    print("=" * 62)

    gen = resilient_worker()
    print("[main] раскрутка:", next(gen))

    print("[main] бросаю внутрь ValueError через gen.throw(...)")
    # throw возобновляет генератор, но вместо значения в точке yield
    # возбуждается наше исключение. Генератор его поймал и вернул новый yield.
    resumed = gen.throw(ValueError("что-то пошло не так"))
    print("[main] генератор выжил и снова готов:", repr(resumed))

    print("[main] после обработки исключения он работает как ни в чём не бывало")
    print("[main] обычный send() снова проходит:", repr(gen.send("нормальная")))
    print()


# =====================================================================
# ЧАСТЬ 5. .close() — вежливая остановка через GeneratorExit
# =====================================================================


def worker_with_cleanup() -> Generator[int, None, None]:
    """Показывает, что при .close() внутрь прилетает GeneratorExit,
    и блок finally успевает прибрать ресурсы (закрыть файл, соединение)."""
    print("  [внутри] ресурс захвачен (представь: открыт файл/сокет)")
    try:
        n = 0
        while True:
            yield n
            n += 1

    except GeneratorExit:
        # .close() бросает сюда GeneratorExit именно в точке yield.
        print("  [внутри] пришёл GeneratorExit — просят закрыться, ок")

    finally:
        # finally выполнится при любом финале — идеальное место для уборки.
        print("  [внутри] finally: ресурс освобождён (файл/сокет закрыт)")


def part5_close() -> None:
    print("=" * 62)
    print("ЧАСТЬ 5. .close(): корректная остановка и уборка (finally)")
    print("=" * 62)

    gen = worker_with_cleanup()
    print("[main] первое значение:", next(gen))
    print("[main] второе значение:", next(gen))

    print("[main] вызываю gen.close() — прошу генератор завершиться")
    gen.close()

    # После close() генератор мёртв: next() сразу даёт StopIteration.
    try:
        next(gen)

    except StopIteration:
        print("[main] после close() генератор исчерпан — next() даёт StopIteration")

    print()


def main() -> None:
    part1_creation_and_stepping()
    part2_state_survives()
    part3_stopiteration()
    part4_throw()
    part5_close()

    print("=" * 62)
    print("АГА-МОМЕНТ")
    print("=" * 62)
    print("Генератор — это функция, которую можно ПОСТАВИТЬ НА ПАУЗУ (yield),")
    print("сохранив все её локальные переменные, а потом РАЗМОРОЗИТЬ с того же")
    print("места (next/send). Наружу можно не только отдавать значения, но и")
    print("вбрасывать их внутрь (send) и даже вбрасывать исключения (throw).")
    print("Ровно этот механизм «заморозил-возобновил» лежит под async-корутинами:")
    print("event loop ставит корутину на паузу на await и будит, когда готов")
    print("результат. Поняв паузу генератора, ты понял сердце asyncio.")


if __name__ == "__main__":
    main()


# Следующий урок: 02 — yield как двусторонний канал: наружу через yield, внутрь через send (прообраз await)
