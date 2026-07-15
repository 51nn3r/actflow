"""Урок 07 — настоящий неблокирующий IO без занятого потока.

Учит: как «ждать данные из сокета», не замораживая поток — через setblocking(False),
selectors.DefaultSelector и мини-loop, который паркует корутину в ОС-селекторе и уступает ход.
Ага-момент: «ожидание IO» = регистрация в селекторе + уступка, а НЕ занятый поток — оттого один поток тянет тысячи сокетов.
"""

import selectors
import socket
from collections import deque
from typing import Any, Generator


# =====================================================================
# ЧАСТЬ 1. Блокирующий recv занял бы поток; неблокирующий — нет
# =====================================================================


def part1_blocking_vs_nonblocking() -> None:
    """Показываем на socketpair разницу между блокирующим и неблокирующим recv."""
    print("=== Часть 1. Блокирующий recv занял бы поток, неблокирующий — нет ===")
    left, right = socket.socketpair()
    print("socket.socketpair() дал пару связанных сокетов — без сети, без портов, безопасно.")
    print("Договорённость: пишем в left → читаем из right.\n")

    # По умолчанию сокет БЛОКИРУЮЩИЙ: right.recv() на пустом сокете заморозил бы весь
    # поток до прихода байтов. Мы НАРОЧНО не делаем такой вызов на пустом сокете —
    # иначе демо повисло бы. Вместо этого включаем неблокирующий режим и смотрим итог.
    right.setblocking(False)
    print("right.setblocking(False): recv больше не ждёт, а сразу возвращает управление.")
    try:
        right.recv(1024)
    except BlockingIOError:
        # errno EAGAIN/EWOULDBLOCK: «данных пока нет, зайди позже» — но поток свободен!
        print("recv на ПУСТОМ неблокирующем сокете → BlockingIOError: 'данных пока нет'.")
        print("Блокирующий сокет здесь заморозил бы поток целиком — вот она, цена ожидания.\n")

    # Теперь данные есть — тот же неблокирующий recv спокойно их отдаёт.
    left.sendall("привет".encode())
    print("left.sendall(...): положили байты в сокет.")
    data = right.recv(1024)
    print(f"right.recv(1024) → {data.decode()!r}: данные готовы, читаем без блокировки.")

    left.close()
    right.close()
    print()


# =====================================================================
# ЧАСТЬ 2. selectors: спросить у ОС, какие сокеты готовы прямо сейчас
# =====================================================================


def part2_selector_basics() -> None:
    """Регистрируем сокет в селекторе и опрашиваем готовность через select(timeout)."""
    print("=== Часть 2. selectors: спросить у ОС, какие сокеты готовы ===")
    left, right = socket.socketpair()
    right.setblocking(False)
    sel = selectors.DefaultSelector()
    # register(fileobj, events, data): просим ОС следить за right на событие «стал читаемым».
    # data — любая метка, которую селектор вернёт нам вместе с готовым сокетом (пригодится в loop).
    sel.register(right, selectors.EVENT_READ, data="right-канал")
    print("Зарегистрировали right на EVENT_READ. Один select() опросит его — и хоть тысячу таких.\n")

    print("select(timeout=0), пока данных нет:")
    events = sel.select(timeout=0)
    print(f"  готовых сокетов: {len(events)} — никто не готов, поток свободен заняться другим.\n")

    left.sendall("данные".encode())
    print("Записали байты в left. Повторяем опрос (timeout — страховка от вечного ожидания):")
    events = sel.select(timeout=0.5)
    for key, _mask in events:
        # key.data — наша метка; key.fileobj — сам сокет, теперь точно готовый к чтению.
        print(f"  готов сокет {key.data!r} → читаю: {key.fileobj.recv(1024).decode()!r}")

    sel.unregister(right)
    sel.close()
    left.close()
    right.close()
    print("\nВЫВОД: select — это 'кто из зарегистрированных готов прямо сейчас?' с таймаутом-страховкой.\n")


# =====================================================================
# ЧАСТЬ 3. Awaitable-заявки для мини-loop
# =====================================================================


class WaitRead:
    """Заявка loop'у: «припаркуй меня, пока в этом сокете не появятся данные».

    __await__ отдаёт саму заявку наружу (`yield self`) — ровно как AwaitRequest в actflow.
    Обратно через .send(data) loop кладёт уже прочитанные байты, и они становятся
    результатом `await WaitRead(sock)`. Корутина при этом НЕ держит поток — она заморожена.
    """

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock

    def __await__(self) -> Generator["WaitRead", bytes, bytes]:
        # yield self = «вот моя заявка, разбуди меня по готовности»; loop вернёт байты через .send().
        data = yield self
        return data


class Yield:
    """`await Yield()` — «уступаю ход» (из урока 05). Отдаём наружу None, а не заявку на IO."""

    def __await__(self) -> Generator[None, Any, None]:
        yield


# =====================================================================
# ЧАСТЬ 3 (продолжение). Корутины-задачи для мини-loop
# =====================================================================


async def reader(name: str, sock: socket.socket) -> None:
    """Читатель: вместо блокирующего recv просит loop разбудить его, когда придут байты."""
    print(f"    [{name}] нужны данные из сокета. НЕ зову блокирующий recv (занял бы поток),")
    print(f"    [{name}] а прошу loop разбудить меня по готовности → await WaitRead(...)")
    data = await WaitRead(sock)
    print(f"    [{name}] проснулся! loop подложил уже прочитанные данные: {data.decode()!r}")


async def cpu_worker(name: str, rounds: int) -> None:
    """Счётная задача: крутит шаги, уступая ход, — работает, пока читатели спят в селекторе."""
    for step in range(1, rounds + 1):
        print(f"    [{name}] считаю шаг {step}/{rounds} (пока читатели спят в селекторе)")
        await Yield()

    print(f"    [{name}] досчитал — завершаюсь")


async def producer(writers: list[tuple[str, socket.socket]]) -> None:
    """Producer: по очереди пишет в сокеты, эмулируя приход данных «из сети»."""
    for label, wsock in writers:
        # Уступаем ход перед записью: пусть читатели успеют припарковаться, а cpu — посчитать.
        await Yield()
        message = f"данные для {label}".encode()
        wsock.sendall(message)
        print(f"    [producer] отправил {len(message)} байт в сокет {label} — читатель скоро проснётся")


# =====================================================================
# ЧАСТЬ 3 (ядро). Мини event loop: селектор вместо блокирующего recv
# =====================================================================


def run_io_loop(tasks: list[tuple[str, Any]], timeout: float = 1.0, max_ticks: int = 10000) -> None:
    """Крошечный event loop, который ждёт IO через селектор ОС, а не через занятый поток.

    Каждый тик:
      ФАЗА 1 — спросить ОС, какие сокеты готовы (select). Если есть чем заняться — заглянуть
               мгновенно (timeout=0); если делать нечего — припарковать ВЕСЬ поток в ОС до
               готовности любого из сокетов (timeout>0). Это и есть «ожидание IO без потока».
      ФАЗА 2 — разбудить корутины, чьи сокеты стали читаемыми: прочитать байты и вернуть в очередь.
      ФАЗА 3 — прокрутить одну готовую корутину до следующей заявки (.send()).
      ФАЗА 4 — разобрать заявку: WaitRead → зарегистрировать сокет в селекторе и заморозить
               корутину; None (await Yield) → просто вернуть в хвост очереди.
    """
    # Очередь готовых бежать: (имя, корутина, что подставить в .send()). Первый send — всегда None.
    ready: deque[tuple[str, Any, Any]] = deque((name, coro, None) for name, coro in tasks)
    sel = selectors.DefaultSelector()
    tick = 0

    while ready or sel.get_map():  # работаем, пока есть готовые ИЛИ кто-то ждёт IO в селекторе
        tick += 1
        if tick > max_ticks:
            print("[loop] АВАРИЙНЫЙ СТОП: превышен лимит тиков (защита от зависания)")
            break

        # --- ФАЗА 1: узнать у ОС, какие сокеты готовы к чтению ---
        if ready:
            # Есть чем заняться — только МГНОВЕННО заглядываем, поток не отдаём.
            events = sel.select(timeout=0) if sel.get_map() else []
        else:
            # Делать нечего — паркуем ВЕСЬ поток в ОС до готовности любого из сокетов.
            # Именно так один поток «ждёт» тысячи соединений, не сжигая CPU в busy-цикле.
            print(f"[loop] очередь задач пуста — паркую поток в ОС-селекторе (жду до {timeout}s)...")
            events = sel.select(timeout=timeout)

            if not events:
                print("[loop] селектор истёк по таймауту без событий — выхожу (защита от зависания)")
                break

        # --- ФАЗА 2: разбудить корутины, чьи сокеты стали читаемыми ---
        for key, _mask in events:
            sock = key.fileobj
            name, coro = key.data
            sel.unregister(sock)  # больше следить за ним не нужно — корутина заберёт данные
            data = sock.recv(1024)  # байты ТОЧНО есть — селектор это гарантировал, recv не блокирует
            print(f"[loop] сокет задачи {name!r} готов, прочитал {len(data)} байт — бужу её")
            ready.append((name, coro, data))  # вернём корутину в очередь с прочитанными данными

        # --- ФАЗА 3: прокрутить одну готовую корутину до следующей заявки ---
        name, coro, send_value = ready.popleft()
        try:
            request = coro.send(send_value)
        except StopIteration:
            print(f"[loop] задача {name!r} завершилась")
            continue

        # --- ФАЗА 4: разобрать заявку корутины ---
        if isinstance(request, WaitRead):
            sock = request.sock
            sock.setblocking(False)  # обязательно: loop сам вызовет recv и не должен блокироваться
            sel.register(sock, selectors.EVENT_READ, data=(name, coro))
            print(f"[loop] задача {name!r} ждёт IO — кладу её сокет в селектор и НЕ держу поток")
        elif request is None:
            # await Yield(): корутина просто уступила ход — вернём в хвост, send-значение не нужно.
            ready.append((name, coro, None))
        else:
            raise RuntimeError(f"неизвестная заявка от корутины: {request!r}")

    sel.close()


def part3a_park_in_os() -> None:
    """Одна задача ждёт IO — loop паркует весь поток в ОС и ОС мгновенно его возвращает."""
    print("=== Часть 3a. Мини-loop паркует поток в ОС до готовности сокета ===")
    left, right = socket.socketpair()
    # Эмулируем «данные уже пришли из сети» — заранее кладём их в сокет.
    left.sendall("сюрприз из сети".encode())
    print("Одна задача reader ждёт данные, больше делать нечего.")
    print("Loop припаркует ВЕСЬ поток в ОС-селекторе — и ОС вернёт его сразу, ведь байты уже в буфере.")
    print("Ни одного busy-цикла, ни одного сожжённого такта CPU на ожидание.\n")

    run_io_loop([("reader", reader("reader", right))], timeout=0.5)

    left.close()
    right.close()
    print("\nВЫВОД: 'ждать IO' = отдать поток ОС через select(), а не крутить recv в цикле.\n")


def part3b_interleaving() -> None:
    """Один поток тянет два сокета И считает — читатели спят в селекторе, cpu работает."""
    print("=== Часть 3b. Один поток обслуживает несколько сокетов И считает между делом ===")
    a_left, a_right = socket.socketpair()
    b_left, b_right = socket.socketpair()
    print("Две 'сети' (два socketpair) + счётная cpu + producer, который пишет в них по очереди.")
    print("Читатели паркуются в селекторе (поток свободен), cpu крутит шаги,")
    print("а как только producer пишет байты — loop на ближайшем select(0) будит нужного читателя.\n")

    tasks = [
        ("readerA", reader("readerA", a_right)),
        ("readerB", reader("readerB", b_right)),
        ("cpu", cpu_worker("cpu", rounds=4)),
        ("producer", producer([("A", a_left), ("B", b_left)])),
    ]
    run_io_loop(tasks, timeout=0.5)

    for s in (a_left, a_right, b_left, b_right):
        s.close()

    print("\nВЫВОД: ОДИН поток тянет и ожидание двух сокетов, и счётную работу — без потока на сокет.")
    print("Масштабируй читателей до тысяч — поток всё равно один: он лишь спрашивает у ОС, кто готов.\n")


def main() -> None:
    print("Настоящий неблокирующий IO: один поток на тысячи сокетов\n")
    part1_blocking_vs_nonblocking()
    part2_selector_basics()
    part3a_park_in_os()
    part3b_interleaving()

    print("ИТОГ УРОКА:")
    print("  1) блокирующий recv на пустом сокете морозит ВЕСЬ поток; неблокирующий кидает BlockingIOError;")
    print("  2) selector регистрирует сокеты в ОС; select(timeout) сообщает, кто готов к чтению;")
    print("  3) 'ожидание IO' в event loop = регистрация в селекторе + уступка хода, а НЕ занятый поток;")
    print("  4) оттого один поток обслуживает тысячи сокетов — это и есть решение 'проблемы №2'.")


if __name__ == "__main__":
    main()


# Следующий урок: 08 — как это делает настоящий asyncio: loop привязан к своему потоку, почему worker'у нужен AwaitRequest
