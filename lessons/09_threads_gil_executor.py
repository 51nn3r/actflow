"""Урок 09 — потоки, GIL, run_in_executor и contextvars.

Учит: почему потоки НЕ ускоряют CPU-код (GIL пускает в байткод один поток за раз),
но спасают блокирующий / GIL-отпускающий код через loop.run_in_executor; и что
contextvars в новый поток сами не текут — их переносят руками copy_context()+context.run().
Ага-момент: context.run(coro.send, ...) в run_fiber_step — это и есть тот самый ручной перенос.
"""

import asyncio
import contextvars
import threading
import time
from concurrent.futures import ThreadPoolExecutor


# =====================================================================
# ЧАСТЬ 1. GIL: один поток в байткоде; но sleep/IO отпускают GIL
# =====================================================================


def cpu_burn(rounds: int) -> int:
    """CPU-bound работа: тесная петля целочисленной арифметики.
    Всё время исполнения держит GIL — другим потокам байткод крутить не даёт."""
    total = 0
    for i in range(rounds):
        total += i * i % 7

    return total


def part1_gil_cpu_vs_sleep() -> None:
    """Замеряем CPU-работу и sleep в 1 поток vs 2 потока — и видим разницу из-за GIL."""
    print("=== Часть 1. GIL: только ОДИН поток исполняет Python-байткод за раз ===")
    rounds = 4_000_000
    cpu_burn(rounds)  # разогрев: первый замер не должен включать прогрев интерпретатора

    # --- 1a. CPU-bound: две порции работы подряд в 1 потоке vs по одной в 2 потоках ---
    start = time.monotonic()
    cpu_burn(rounds)
    cpu_burn(rounds)
    seq = time.monotonic() - start
    print(f"[cpu] 2 порции ПОДРЯД в 1 потоке: {seq:.3f}s (эталон: столько занимает вся работа)")

    # «В лоб» кажется: два потока по одной порции должны управиться примерно вдвое быстрее.
    threads = [threading.Thread(target=cpu_burn, args=(rounds,)) for _ in range(2)]
    start = time.monotonic()
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    par = time.monotonic() - start
    print(f"[cpu] те же 2 порции в 2 ПОТОКАХ:  {par:.3f}s")
    print(f"[cpu] ускорение = {seq / par:.2f}x — ждали ~2.0x, а вышло ~1.0x (а то и хуже)!")
    print("      GIL сериализовал потоки: пока один крутит байткод, второй ждёт своей очереди.\n")

    # --- 1b. time.sleep ОТПУСКАЕТ GIL на время сна — и потоки реально идут параллельно ---
    nap = 0.3
    start = time.monotonic()
    time.sleep(nap)
    time.sleep(nap)
    seq = time.monotonic() - start
    print(f"[sleep] два sleep({nap}) ПОДРЯД в 1 потоке: {seq:.3f}s")

    threads = [threading.Thread(target=time.sleep, args=(nap,)) for _ in range(2)]
    start = time.monotonic()
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    par = time.monotonic() - start
    print(f"[sleep] два sleep({nap}) в 2 ПОТОКАХ:      {par:.3f}s")
    print(f"[sleep] ускорение = {seq / par:.2f}x — вот теперь ~2x: sleep на время сна отдаёт GIL,")
    print("        и второй поток спит ОДНОВРЕМЕННО. Так же ведут себя блокирующий IO и вызовы C-библиотек.")
    print("        ВЫВОД: потоки бесполезны для чистого CPU, но полезны для блокирующего/GIL-отпускающего кода.\n")


# =====================================================================
# ЧАСТЬ 2. run_in_executor: блокирующий код — в поток, event loop свободен
# =====================================================================


def blocking_call(label: str, seconds: float) -> str:
    """Блокирующая функция (эмуляция медленного IO или вызова C-библиотеки).
    Держит СВОЙ поток занятым `seconds` секунд и возвращает результат.
    Внутри event loop её звать нельзя — заморозила бы весь loop."""
    time.sleep(seconds)
    return f"результат[{label}]"


async def heartbeat(stop: asyncio.Event) -> int:
    """Пульс на event loop: тикает, пока не попросят остановиться.
    Доказывает наглядно, что loop НЕ заблокирован, пока worker-поток занят."""
    ticks = 0
    while not stop.is_set():
        ticks += 1
        print(f"    [loop-пульс] тик {ticks}: event loop жив и крутит корутины, пока поток блокируется")
        await asyncio.sleep(0.1)

    return ticks


async def part2_run_in_executor() -> None:
    """Отправляем блокирующую функцию в пул потоков и await'им результат, не морозя loop."""
    print("=== Часть 2. run_in_executor: блокирующий код — в поток, loop свободен ===")
    loop = asyncio.get_running_loop()
    pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="worker")

    stop = asyncio.Event()
    pulse = asyncio.create_task(heartbeat(stop))  # запустим пульс параллельно нашей корутине

    print("Зову blocking_call через loop.run_in_executor: сама функция уедет в worker-поток,")
    print("а run_in_executor СРАЗУ вернёт awaitable Future и отпустит loop (наша корутина заснёт на await).\n")
    result = await loop.run_in_executor(pool, blocking_call, "IO", 0.35)
    print(f"\n[await вернул] {result!r} — поток досчитал и потокобезопасно сделал future.set_result,")
    print("а loop разбудил нашу корутину этим результатом. Пульс выше тикал — loop всё это время не стоял.\n")

    stop.set()
    await pulse  # аккуратно доедаем пульс-корутину

    # Два блокирующих вызова разом: каждый в своём потоке, а sleep отпускает GIL — идут параллельно.
    print("Теперь ДВА блокирующих вызова разом через gather — каждый в отдельном worker-потоке:")
    start = time.monotonic()
    both = await asyncio.gather(
        loop.run_in_executor(pool, blocking_call, "A", 0.3),
        loop.run_in_executor(pool, blocking_call, "B", 0.3),
    )
    elapsed = time.monotonic() - start
    print(f"    оба готовы за {elapsed:.3f}s (~0.3, а не ~0.6): {both}")
    print("    потоки блокировались ОДНОВРЕМЕННО, и loop собрал оба результата через один await gather.\n")

    pool.shutdown(wait=False)


# =====================================================================
# ЧАСТЬ 3. contextvars: значение не течёт в поток само — переносим руками
# =====================================================================


# ContextVar — «переменная текущего контекста»: у каждого потока/задачи своя копия видимости.
current_user: contextvars.ContextVar[str] = contextvars.ContextVar("current_user", default="<аноним>")


def read_user(tag: str, sink: dict) -> None:
    """Читает current_user в том контексте, где реально исполняется, и кладёт результат в sink[tag]."""
    sink[tag] = current_user.get()


def part3_contextvars() -> None:
    """Показываем: обычный поток не видит ContextVar родителя; copy_context()+run переносит его."""
    print("=== Часть 3. contextvars: в поток значение НЕ течёт само — переносим руками ===")
    current_user.set("alice")
    print(f"В главном потоке current_user.set('alice'); здесь current_user.get() = {current_user.get()!r}\n")

    sink: dict[str, str] = {}

    # --- 3a. Обычный поток стартует с ЧИСТЫМ контекстом — родительского значения не видит ---
    t = threading.Thread(target=read_user, args=("naive", sink))
    t.start()
    t.join()
    print(f"[naive] обычный threading.Thread прочитал current_user = {sink['naive']!r}")
    print("        → это DEFAULT, а НЕ 'alice': у нового потока свой, пустой контекст.\n")

    # --- 3b. copy_context() снимает СЛЕПОК текущего контекста; context.run исполняет функцию ВНУТРИ него ---
    ctx = contextvars.copy_context()
    print("copy_context() снял слепок контекста главного потока (в нём current_user='alice').")
    t = threading.Thread(target=ctx.run, args=(read_user, "carried", sink))
    t.start()
    t.join()
    print(f"[carried] тот же код, но обёрнутый в ctx.run(...), прочитал current_user = {sink['carried']!r}")
    print("          → 'alice' доехала в поток, потому что контекст мы перенесли РУКАМИ.\n")

    print("АГА-МОМЕНТ — ровно это делает actflow/fiber.py в run_fiber_step:")
    print("    yielded = context.run(coro.send, value)")
    print("run_in_executor и обычный Thread НЕ копируют contextvars в worker-поток сами.")
    print("Поэтому рантайм хранит fiber.context и гоняет КАЖДЫЙ шаг корутины через context.run —")
    print("так ContextVar фибера доезжают в тот поток, где физически исполняется coro.send.\n")


def main() -> None:
    print("Потоки, GIL, run_in_executor и contextvars: когда потоки нужны и как в них едет контекст\n")
    part1_gil_cpu_vs_sleep()
    asyncio.run(part2_run_in_executor())
    part3_contextvars()

    print("ИТОГ УРОКА:")
    print("  1) GIL: одновременно байткод крутит только ОДИН поток → потоки не ускоряют чистый CPU;")
    print("  2) time.sleep / блокирующий IO / вызовы C-библиотек ОТПУСКАЮТ GIL → там потоки помогают;")
    print("  3) loop.run_in_executor(pool, fn) отправляет блокирующую fn в поток, а loop остаётся свободным;")
    print("  4) contextvars в поток НЕ текут сами — переносим их copy_context()+context.run(),")
    print("     ровно как run_fiber_step гоняет каждый шаг фибера через context.run(coro.send, ...).")


if __name__ == "__main__":
    main()


# Следующий урок: 10 — собираем всё: мини-версия actflow fiber-рантайма в одном файле (worker шагает, central loop ждёт)
