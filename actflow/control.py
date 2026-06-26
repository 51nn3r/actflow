"""Парные контроллеры — обратимая скобка вокруг тела.

Контроллер ввода (вневременной): принимает пакеты в очереди по ярлыкам,
сигналит готовность (poll), собирает входы при запуске (collect).
Контроллер вывода (вневременной): размечает результат тела для раскладки.

Договариваются через квитанцию: collect возвращает «что запомнить»,
исполнитель проносит её до выхода (emit)."""

from __future__ import annotations

from collections import deque

from .core import Ready, Wait


class InputController:
    """Состояние входа узла: по очереди на каждый ожидаемый ярлык.

    Маршрутизация пакета:
      - ярлык совпал с ожидаемым -> в его очередь;
      - ярлык неизвестен -> в первый ещё не связанный слот по порядку.

    poll  — сигнал готовности (по умолчанию: в каждой очереди есть пакет);
    collect — собрать и изъять входы при запуске (по одному из очереди, FIFO)."""

    def __init__(self, labels: tuple[str, ...]):
        self.labels = labels or ("in",)
        self.queues: dict[str, deque] = {lab: deque() for lab in self.labels}
        self._bound: dict[str, str] = {}

    def offer(self, packet):
        self._route(packet)

        return self.poll()

    def _route(self, packet):
        if packet.label in self.queues:
            self.queues[packet.label].append(packet)

            return

        slot = self._bound.get(packet.label) or self._free_slot()
        self._bound[packet.label] = slot
        self.queues[slot].append(packet)

    def _free_slot(self):
        for lab in self.labels:
            if not self.queues[lab] and lab not in self._bound.values():
                return lab

        return self.labels[0]

    def poll(self):
        for lab in self.labels:
            if not self.queues[lab]:
                return Wait()

        return Ready()

    def collect(self):
        # вернуть входы для тела и квитанцию, изъяв их из очередей
        inputs = {lab: self.queues[lab].popleft().value for lab in self.labels}

        return inputs, None


class OutputController:
    """Стандартный выход: помечает результат ярлыком-типом узла,
    если задача не указала ярлык явно. receipt не используется."""

    def __init__(self, type_label: str):
        self.type_label = type_label

    def emit(self, results, receipt):
        out = []
        for r in results:
            label = r.label if r.label is not None else self.type_label
            out.append((r.value, r.node, label))

        return out
