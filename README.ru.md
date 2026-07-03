# actflow

[English](README.md)

Исполнение графов задач с двухуровневой моделью узла. Тело задачи — чистая функция одного такта; контроллеры ввода/вывода — скобка с состоянием вокруг него, отвечающая за готовность, порядок и батчи между тактами.

Полное описание модели — [SPEC.ru.md](SPEC.ru.md).

## Структура

```
actflow/
  core.py       Packet, Collected, TaskResult, исходы готовности, context vars
  control.py    InputController, OutputController, OrderedInputController
  node.py       Node, LinkRef — узел графа и async-исполнение
  task.py       базовый класс Task
  tasks.py      готовые задачи: Input, Terminal, Tap
  executor.py   SyncExecutor, AsyncExecutor, ExecutorHandle

examples/
  01_montecarlo_lasvegas.py   гонка проверов, отмена соседа
  02_redis_batcher.py         батч по размеру и таймауту, async-тело
  03_distributed_layers.py    распределённые слои НС, синхронизатор порядка
  04_naming_variants.py       спектр именования: input_map, output_map, slot_map
```

## Запуск

```
cd actflow && pip install -e . && python examples/01_montecarlo_lasvegas.py
```

## API

### Тело задачи

`execute` принимает именованные параметры (сопоставляются с входными слотами по ярлыку источника) и возвращает словарь маршрутизации или список `TaskResult`:

```python
class Double(Task):
    def execute(self, value) -> dict:
        return {"next": value * 2}
```

Ключ `None` отправляет значение на выход графа:

```python
class Terminal(Task):
    def execute(self, value) -> dict:
        return {None: value}
```

Async-тела прозрачны — исполнитель await-ит их автоматически:

```python
class Fetch(Task):
    async def execute(self, url) -> dict:
        data = await http_get(url)
        return {"next": data}
```

### Сборка графа

`task()` создаёт узел; `>>` соединяет узлы; `["name"]` выбирает именованный выходной сокет:

```python
inp = Input()()
dbl = Double()()
end = Terminal()()

inp >> dbl >> end

SyncExecutor().run(inp, 21)   # → [42]
```

Именованные сокеты и петли:

```python
class Retry(Task):
    def execute(self, item) -> dict:
        if done(item):
            return {"out": item}
        return {"retry": item}

node = Retry()()
node["retry"] >> node   # петля на себя
node["out"] >> sink
```

### Ярлык источника

Ярлык источника штампуется на каждый исходящий пакет и направляет его в нужный входной слот получателя. Задаётся в `__init__`; по умолчанию — имя класса:

```python
w1 = Worker(label="w1")()
w2 = Worker(label="w2")()

class Collector(Task):
    def execute(self, w1, w2) -> dict:   # имена слотов совпадают с ярлыками
        ...
```

Для простого графа (один узел на класс) ничего указывать не нужно — имена классов уникальны по умолчанию.

### Управление именами

Слоты выводятся из параметров `execute`; при необходимости имена переопределяются или расширяются:

```python
Merge(in_labels=("left", "right"))  # объявить слоты (у **kwargs-тела их не вывести)
Two(input_map={"A": "a", "B": "b"})  # прыжок 1: ярлык ребра -> слот задачи
Split(output_map={"hi": "H", "lo": "L"})  # пер-линковый исходящий ярлык
Two(input_map={"A": "a"}, on_dropped=log)  # хук на неучтённый ярлык
InputController(("a", "b"), slot_map={"a": "q1", "b": "q2"})  # прыжок 2: слот -> очередь
```

Полный спектр — `examples/04_naming_variants.py`.

### Состояние и управление

Внутри `execute` доступны память узла и рычаги исполнителя через `self`:

```python
class Counter(Task):
    def execute(self, value) -> dict:
        self.memory["n"] = self.memory.get("n", 0) + 1
        if self.memory["n"] >= 10:
            self.stop()
        return {"next": value}
```

Fan-out на одну и ту же связь через `self.to()`:

```python
class Broadcast(Task):
    async def execute(self, items) -> list:
        return [self.to("out", item) for item in items]
```

### Кастомные контроллеры

Передайте кастомный контроллер ввода в `__init__` для батчинга, упорядочения и т.д.:

```python
batcher = Batcher(input_controller=BatchInputController(("batch",)))()
collect = Collect(input_controller=OrderedInputController(("done",)))()
```

`OrderedInputController` встроен — доставляет пакеты в строгом порядке возрастания поля `idx` и передаёт `mark={"idx": n}` контроллеру вывода.
