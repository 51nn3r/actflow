# actflow

[English](README.md)

Исполнение графов задач с двухуровневой моделью узла. Тело задачи — чистая функция одного такта; контроллеры ввода/вывода — скобка с состоянием вокруг него, которая отвечает за готовность, порядок и батчи между тактами.

Полное описание модели — [SPEC.ru.md](SPEC.ru.md).

## Структура

```
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
```

## Запуск

```
PYTHONPATH=. python examples/01_montecarlo_lasvegas.py
```

## API

Задача считает и адресует результат по имени связи:

```python
class Double(Task):
    def execute(self, inputs, ctx):
        x = next(iter(inputs.values()))
        return [ctx.to("next", x * 2)]
```

Граф собирается из узлов с именованными связями (связь на себя — цикл):

```python
inp = Input()()
dbl = Double()()
end = Terminal()()
inp.link("next", dbl)
dbl.link("next", end)

SyncExecutor().run(inp, 21)   # → [42]
```

Тело может быть async — долгая или удалённая работа прозрачна:

```python
class Remote(Task):
    async def execute(self, inputs, ctx):
        data = await redis_call(...)
        return [ctx.to("next", data)]
```

Кастомное поведение приёма (батч, порядок) — подмена контроллера ввода:

```python
node.input = BatchInput(("batch",))
```

Управление выражается узлами графа через `ctx.control`:

```python
Tap(lambda ctx, v: ctx.control.stop())
Tap(lambda ctx, v: print(ctx.control.snapshot()))
```