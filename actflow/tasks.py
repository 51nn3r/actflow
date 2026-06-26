"""Готовые задачи общего назначения: вход, терминал, управляющий узел."""

from __future__ import annotations

from .task import Task


class Input(Task):
    """Точка входа графа: пробрасывает поданное значение в связь 'next'."""

    def execute(self, inputs, ctx):
        value = next(iter(inputs.values()))

        return [ctx.to("next", value)]


class Terminal(Task):
    """Терминал: выводит пришедшее значение наружу графа."""

    def execute(self, inputs, ctx):
        value = next(iter(inputs.values()))

        return [ctx.out(value)]


class Tap(Task):
    """Управляющий узел: выполняет действие (лог, метрика, сторож, остановка)
    и прозрачно пробрасывает значение в связь 'next'.
    action получает (ctx, value) и может дёргать ctx.control."""

    def __init__(self, action):
        self.action = action

    def execute(self, inputs, ctx):
        value = next(iter(inputs.values()))
        self.action(ctx, value)

        return [ctx.to("next", value)]
