"""Пример 3. Распределённое выполнение слоёв НС.

Несколько рабочих узлов считают слои сети. Как только рабочий освобождается,
он берёт следующую пару [слой, ввод] и считает. Слои идут последовательно:
выход слоя i — вход слоя i+1.

Параллельность здесь — между независимыми образцами в потоке: пока рабочий
считает слой образца A, другой может считать тот же слой образца B. Чтобы
ответы вышли в исходном порядке, на выходе стоит синхронизатор — пара
контроллеров: вход помнит порядковый номер, выход отдаёт по возрастанию.

Показывает: распределённую раздачу работы по мере освобождения, async-тела
слоёв с разной задержкой, синхронизатор порядка как парные контроллеры,
квитанцию вход->выход."""

import asyncio
import random

from actflow import Task, AsyncExecutor
from actflow.control import InputController, OutputController
from actflow.core import Ready, Wait


LAYERS = 3                      # глубина сети
SAMPLES = 6                     # образцов в потоке


class Feed(Task):
    """Подаёт образцы в сеть, помечая порядковым номером для синхронизатора."""

    async def execute(self, inputs, ctx):
        items = next(iter(inputs.values()))
        out = []
        for idx, x in enumerate(items):
            out.append(ctx.to("layer", {"idx": idx, "value": x, "layer": 0}))

        return out


class Layer(Task):
    """Один слой: считает с переменной задержкой (распределённый рабочий).
    Пока не последний слой — отправляет результат себе же на следующий слой;
    иначе — в синхронизатор вывода."""

    async def execute(self, inputs, ctx):
        item = next(iter(inputs.values()))
        await asyncio.sleep(random.uniform(0.01, 0.05))   # неравномерная работа

        item = dict(item)
        item["value"] = item["value"] * 2 + 1             # «вычисление слоя»
        item["layer"] += 1

        if item["layer"] < LAYERS:
            return [ctx.to("layer", item)]                # следующий слой

        return [ctx.to("done", item)]                     # готово -> синхронизатор


class OrderedInput(InputController):
    """Вход синхронизатора: пропускает результаты строго по возрастанию idx.
    Придерживает пришедшие не по порядку, отдаёт, когда подходит их черёд.
    Запомненный idx уходит в квитанции на выход."""

    def __init__(self, labels):
        super().__init__(labels)
        self._slot = self.labels[0]
        self._next = 0
        self._held: dict = {}

    def offer(self, packet):
        item = packet.value
        self._held[item["idx"]] = item

        return self.poll()

    def poll(self):
        return Ready() if self._next in self._held else Wait()

    def collect(self):
        item = self._held.pop(self._next)
        idx = self._next
        self._next += 1

        return {self._slot: item}, idx        # квитанция — порядковый номер


class OrderedOutput(OutputController):
    """Выход синхронизатора: вешает запомненный idx (из квитанции) обратно
    на результат. Здесь — просто метит, порядок уже восстановлен входом."""

    def emit(self, results, receipt):
        out = []
        for r in results:
            out.append((r.value, r.node, self.type_label))

        return out


class Collect(Task):
    """Терминал-синхронизатор: выводит наружу значения в исходном порядке."""

    def execute(self, inputs, ctx):
        item = next(iter(inputs.values()))

        return [ctx.out((item["idx"], item["value"]))]


def build():
    feed = Feed()()
    layer = Layer()()                         # один узел-рабочий, много активаций
    collect = Collect()()
    collect.input = OrderedInput(("done",))   # синхронизатор порядка
    collect.output = OrderedOutput("Collect")

    feed.link("layer", layer)
    layer.link("layer", layer)                # слой за слоем на том же узле
    layer.link("done", collect)

    return feed


async def main():
    print(f"сеть из {LAYERS} слоёв, {SAMPLES} образцов в потоке")
    feed = build()
    ex = AsyncExecutor(max_parallel=4)
    result = await ex.run(feed, list(range(SAMPLES)))

    print("выход (в исходном порядке):", result)
    print("упорядочен:", result == sorted(result))
    print("состояние:", ex.snapshot())


if __name__ == "__main__":
    asyncio.run(main())
