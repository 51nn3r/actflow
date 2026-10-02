"""actflow example 01: a Las Vegas answer from two one-sided Monte Carlo provers.

Two provers race, one per claim; only the one matching the truth can find a
witness, and the first witness out is the answer: always correct, random time.
"""

import asyncio
import random
from contextlib import aclosing

from actflow import Downstream, Executor, GraphOutput, Task

TRUTH = True
WITNESS_CHANCE = 0.25


class Fork(Task):
    """Spawns both provers by emitting one trigger on each named output."""

    def execute(self, trigger):
        yield Downstream(None, output="t")
        yield Downstream(None, output="f")


class Prover(Task):
    """One-sided Monte Carlo: emits a witness on 'win' only when claim equals
    truth and the coin lands, otherwise sends the trigger back around 'retry'."""

    def __init__(self, claim, truth, **kwargs):
        super().__init__(**kwargs)
        self.claim = claim
        self.truth = truth
        self.tries = 0

    def execute(self, trigger):
        self.tries += 1
        if self.claim == self.truth and random.random() < WITNESS_CHANCE:
            return Downstream(self.claim, output="win")

        return Downstream(None, output="retry")


class Decide(Task):
    """Hands the first proven claim out of the graph."""

    def execute(self, value):
        return GraphOutput(value)


def build(truth):
    true_prover = Prover(True, truth)
    false_prover = Prover(False, truth)

    fork = Fork()()
    proof_true = true_prover()
    proof_false = false_prover()
    decide = Decide()()

    fork["t"] >> proof_true
    fork["f"] >> proof_false
    # failed tries feed each prover its own trigger again
    proof_true["retry"] >> proof_true
    proof_false["retry"] >> proof_false
    # both provers race into the same decide node
    proof_true["win"] >> decide
    proof_false["win"] >> decide

    return fork, true_prover, false_prover


async def main():
    print(f"monte carlo to las vegas (truth = {TRUTH})")
    fork, true_prover, false_prover = build(TRUTH)
    executor = Executor(max_parallel=4)

    # closing the stream after the first answer cancels the losing prover's retry loop
    async with aclosing(executor.run(fork, None)) as answers:
        answer = await anext(answers)

    winner, loser = (true_prover, false_prover) if answer else (false_prover, true_prover)
    print(f"las vegas answer: {answer}")
    print(f"claim {winner.claim} won on try {winner.tries}; "
          f"claim {loser.claim} never found a witness (tries: {loser.tries})")


if __name__ == "__main__":
    asyncio.run(main())
