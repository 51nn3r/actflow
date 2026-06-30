import asyncio
import random

from actflow import Task, AsyncExecutor


class Fork(Task):
    """Spawns both provers."""

    def execute(self, trigger) -> dict:
        return {"t": None, "f": None}


class Prover(Task):
    """One-sided Monte Carlo: produces a witness only when claim == truth,
    otherwise retries via the 'retry' self-loop."""

    def __init__(self, claim, truth, p=0.25, **kwargs):
        super().__init__(**kwargs)
        self.claim = claim
        self.truth = truth
        self.p = p

    def execute(self, trigger) -> dict:
        self.memory["tries"] = self.memory.get("tries", 0) + 1
        if self.claim == self.truth and random.random() < self.p:
            return {"win": self.claim}

        return {"retry": None}


class Decide(Task):
    """First winner stops the executor and emits the proven answer."""

    def execute(self, value) -> dict:
        self.stop()
        return {None: value}


def build(truth):
    fork = Fork()()
    pt = Prover(True, truth, label="prover_true")()
    pf = Prover(False, truth, label="prover_false")()
    decide = Decide()()

    fork["t"] >> pt
    fork["f"] >> pf
    pt["retry"] >> pt  # self-loop for retries
    pf["retry"] >> pf
    pt["win"] >> decide  # both provers race to the same decide node
    pf["win"] >> decide

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