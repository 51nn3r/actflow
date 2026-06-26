"""Базовые типы: пакет данных, адресованный результат, исходы готовности."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True)
class Packet:
    """Конверт над значением. Неизменяем: один результат можно направить
    в несколько узлов без копий.

    value — нагрузка, с ней работает тело;
    label — ярлык типа, адрес слота в получателе."""

    value: Any
    label: str

    def relabel(self, label: str) -> "Packet":
        # смена ярлыка = выбор другого слота; нагрузка та же
        return replace(self, label=label)


@dataclass(frozen=True)
class TaskResult:
    """Адресованный результат тела: данные и узел-получатель.
    Слот внутри получателя выбирается по ярлыку (label) при доставке."""

    value: Any
    node: "Node"
    label: str | None = None     # None — пометить ярлыком-типом узла-источника


# ── исходы опроса готовности (что контроллер ввода отвечает на пакет) ──

class Verdict:
    """База для исходов готовности."""


@dataclass(frozen=True)
class Ready(Verdict):
    """Узел готов к запуску. Сигнал готовности; входы собирает collect()
    в момент запуска (а не при опросе), чтобы накопление в очереди между
    опросами не плодило устаревшие снимки."""


@dataclass(frozen=True)
class Wait(Verdict):
    """Не готов; ждать только новых данных."""


@dataclass(frozen=True)
class WaitUntil(Verdict):
    """Не готов, но разбудить не позже момента T (дедлайн батча)."""

    deadline: float
