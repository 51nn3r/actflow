"""Узел графа и контекст вызова тела."""

from __future__ import annotations

from .core import TaskResult
from .control import InputController, OutputController


class Node:
    """Место задачи в графе: контроллеры ввода/вывода, тело и связи.
    Связи именованы: задача в теле просит узел-получателя по имени связи,
    поэтому сама задача переиспользуема и конкретных узлов не хранит."""

    def __init__(self, task, labels, type_label, name=""):
        self.task = task
        self.name = name or type(task).__name__
        self.input = InputController(tuple(labels))
        self.output = OutputController(type_label)
        self.links: dict[str, "Node"] = {}

    def link(self, name, target) -> "Node":
        # связать именованный выход с узлом-получателем (в т.ч. с собой)
        self.links[name] = target

        return self

    def __repr__(self):
        return f"<Node {self.name}>"


class Ctx:
    """Окружение одного запуска тела. Даёт телу:
      link(name)        — узел-получатель по имени связи;
      to(name, value)   — готовый адресованный результат в эту связь;
      out(value)        — терминальный результат наружу (через связь '_out');
      memory            — локальная память узла между тактами;
      control           — рычаги управления исполнителем."""

    def __init__(self, node, control):
        self._node = node
        self.control = control

    @property
    def memory(self) -> dict:
        return self._node.__dict__.setdefault("_memory", {})

    def link(self, name) -> Node:
        return self._node.links[name]

    def to(self, name, value, label=None) -> TaskResult:
        return TaskResult(value, self._node.links[name], label)

    def out(self, value) -> TaskResult:
        # узел-получатель None — исполнитель выведет значение наружу графа
        return TaskResult(value, None, None)
