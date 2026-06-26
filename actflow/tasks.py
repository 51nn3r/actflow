from __future__ import annotations

from .task import Task


class Input(Task):
    """Graph entry point: forwards the received value through the 'next' link."""

    def execute(self, inputs, ctx):
        value = next(iter(inputs.values()))

        return [ctx.to("next", value)]


class Terminal(Task):
    """Emits the received value as a graph output."""

    def execute(self, inputs, ctx):
        value = next(iter(inputs.values()))

        return [ctx.out(value)]


class Tap(Task):
    """Runs a side-effect action then forwards the value through the 'next' link.
    action(ctx, value) — may call ctx.control to steer the executor."""

    def __init__(self, action):
        self.action = action

    def execute(self, inputs, ctx):
        value = next(iter(inputs.values()))
        self.action(ctx, value)

        return [ctx.to("next", value)]
