"""Базовый класс задачи. Вызов задачи создаёт узел графа, не исполняя его."""

from __future__ import annotations

from .node import Node


class Task:
    """Единица вычисления. Переопредели execute.

    execute(inputs, ctx) -> список TaskResult, где inputs — словарь
    {ярлык: значение}, собранный контроллером ввода, а ctx адресует
    результаты по именам связей.

    in_labels  — ярлыки входных слотов (что узел принимает);
    type_label — ярлык результата по умолчанию.
    Тело может быть обычной функцией (sync) или корутиной (async)."""

    in_labels: tuple = ()
    type_label: str = ""

    def execute(self, inputs: dict, ctx) -> list:
        raise NotImplementedError

    def __call__(self, name="") -> Node:
        labels = self.in_labels
        type_label = self.type_label or type(self).__name__

        return Node(self, labels, type_label, name=name)
