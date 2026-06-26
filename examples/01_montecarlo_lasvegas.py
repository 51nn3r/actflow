"""Пример 1. Монте-Карло -> Лас-Вегас: гонка двух проверов.

Два односторонних вероятностных провера крутятся параллельно. Каждый может
«доказать» ответ, только если его утверждение совпадает с истиной, и то с
вероятностью p за попытку. Если истина True, провер с утверждением False не
докажет никогда — его снимает победитель.

Снятие соседа выражено узлом графа: первый дошедший до решающего узла
останавливает исполнитель, и цикл проигравшего обрывается.

Показывает: параллельную гонку, цикл через связь узла на себя,
остановку соседней ветви управляющим узлом."""

import asyncio
import random

from actflow import Task, AsyncExecutor


class Fork(Task):
    """Запускает обоих проверов."""

    def execute(self, inputs, ctx):
        return [ctx.to("t", None), ctx.to("f", None)]


class Prover(Task):
    """Односторонний Монте-Карло: свидетельствует, лишь когда claim == truth.
    Иначе уходит на повтор по связи 'retry' (она ведёт на сам узел)."""

    def __init__(self, claim, truth, p=0.25):
        self.claim = claim
        self.truth = truth
        self.p = p

    def execute(self, inputs, ctx):
        ctx.memory["tries"] = ctx.memory.get("tries", 0) + 1
        if self.claim == self.truth and random.random() < self.p:
            return [ctx.to("win", self.claim)]

        return [ctx.to("retry", None)]


class Decide(Task):
    """Управляющий узел: первый победитель останавливает исполнитель
    и выводит доказанный ответ наружу."""

    def execute(self, inputs, ctx):
        value = next(iter(inputs.values()))
        ctx.control.stop()

        return [ctx.out(value)]


def build(truth):
    fork = Fork()()
    pt = Prover(True, truth)("prover_true")
    pf = Prover(False, truth)("prover_false")
    decide = Decide()()

    fork.link("t", pt)
    fork.link("f", pf)
    pt.link("retry", pt)             # цикл-повтор: узел на себя
    pf.link("retry", pf)
    pt.link("win", decide)           # оба ведут в один узел; сработает первый
    pf.link("win", decide)

    return fork


async def main():
    print("Монте-Карло -> Лас-Вегас (истина = True)")
    fork = build(truth=True)
    ex = AsyncExecutor(max_parallel=4)
    result = await ex.run(fork, None)

    print("Лас-Вегас ответ:", result)
    print("состояние:", ex.snapshot())


if __name__ == "__main__":
    asyncio.run(main())
