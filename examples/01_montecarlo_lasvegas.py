import asyncio
import random

from actflow import Task, AsyncExecutor


class Fork(Task):
    """Spawns both provers."""

    def execute(self, inputs, ctx):
        return [ctx.to("t", None), ctx.to("f", None)]


class Prover(Task):
    """One-sided Monte Carlo: produces a witness only when claim == truth,
    otherwise retries via the 'retry' self-loop."""

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
    """First winner stops the executor and emits the proven answer."""

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
    pt.link("retry", pt)  # self-loop for retries
    pf.link("retry", pf)
    pt.link("win", decide)  # both provers race to the same decide node
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
