# actflow

Исполнение графов задач с двухуровневой моделью узла. Тело задачи — чистая
функция одного такта; контроллеры ввода/вывода — вневременная скобка вокруг
него, которая держит состояние между тактами (готовность, порядок, батчи).

Полное описание модели — в [SPEC.md](SPEC.md).

## Структура

    actflow/
      core.py       пакет данных, адресованный результат, исходы готовности
      control.py    контроллеры ввода/вывода (стандартные реализации)
      node.py       узел графа и контекст вызова тела
      task.py       базовый класс задачи
      tasks.py      готовые задачи: Input, Terminal, Tap
      executor.py   синхронный и асинхронный исполнители, рычаги управления

    examples/
      01_montecarlo_lasvegas.py   гонка проверов, отмена соседа
      02_redis_batcher.py         батч по размеру и таймауту, async-тело
      03_distributed_layers.py    распределённые слои НС, синхронизатор порядка

## Запуск

    PYTHONPATH=. python examples/01_montecarlo_lasvegas.py

## Краткий обзор API

Задача считает и адресует результат по имени связи:

    class Double(Task):
        def execute(self, inputs, ctx):
            x = next(iter(inputs.values()))
            return [ctx.to("next", x * 2)]

Граф собирается из узлов, связи именованы (в т.ч. на себя — для циклов):

    inp = Input()()
    dbl = Double()()
    end = Terminal()()
    inp.link("next", dbl)
    dbl.link("next", end)

    SyncExecutor().run(inp, 21)          # -> [42]

Тело может быть async — долгая или удалённая работа прозрачна:

    class Remote(Task):
        async def execute(self, inputs, ctx):
            data = await redis_call(...)
            return [ctx.to("next", data)]

Кастомное поведение приёма (батч, порядок) — подмена контроллера ввода:

    node.input = BatchInput(("batch",))   # копит до размера или таймаута

Управление — узлами внутри графа через ctx.control:

    Tap(lambda ctx, v: ctx.control.stop())
    Tap(lambda ctx, v: print(ctx.control.snapshot()))
