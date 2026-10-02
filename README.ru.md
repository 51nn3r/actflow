# actflow

[English](README.md)

Версия 0.2.0.

Исполнение графов задач с двухуровневой моделью узла. Тело задачи — чистая функция одного такта, испускающая значения-результаты; контроллеры ввода и вывода — скобка с состоянием вокруг него, решающая готовность, порядок и батчи между тактами. Топология решает, куда идёт значение, контроллеры — когда, задачи — что, а исполнитель применяет изменения состояния.

## Установка

```
pip install -e .
```

## Быстрый старт

```python
import asyncio
from actflow import Downstream, Executor, GraphOutput, Task


class Double(Task):
    def execute(self, value):
        return Downstream(value * 2)


class Emit(Task):
    def execute(self, value):
        return GraphOutput(value)


async def main():
    double = Double()()
    emit = Emit()()
    double >> emit
    async for out in Executor().run(double, 21):
        print(out)


asyncio.run(main())
```

Печатает `42`.

## Тело задачи

Наследуйте `Task` и определите `execute` с именованными параметрами; имена входов выводятся из сигнатуры, поэтому variadic-тело падает при построении узла. Тело испускает объекты-результаты: `Downstream(value, output="next")` идёт дальше по графу, `GraphOutput(value)` покидает его, а возврат `None` не испускает ничего. Работают четыре формы тела:

```python
class Plain(Task):
    def execute(self, value):
        return Downstream(value)


class Coro(Task):
    async def execute(self, url):
        return Downstream(await fetch(url))


class Gen(Task):
    def execute(self, batch):
        for item in batch:
            yield Downstream(item)


class AsyncGen(Task):
    async def execute(self, url):
        async for chunk in stream(url):
            yield GraphOutput(chunk)
```

## Сборка графа

Вызов экземпляра задачи строит узел; `>>` соединяет узлы, `["name"]` выбирает порт с любой стороны:

```python
a = A()()
b = B()()

a >> b  # every output of a into every input of b
a >> b["left"]  # every output into one input
a["hot"] >> b  # one output into every input
a["hot"] >> b["left"]  # one output into one input
```

Имена входов известны из `execute`, поэтому правая сторона `>>` проверяется строго. Выходы нигде не объявляются: тело может испустить любое имя выхода, а значение на неподключённом выходе молча отбрасывается. Дубли рёбер схлопываются (цели — множества), петли на себя разрешены (`node["retry"] >> node`), а рёбра можно добавлять из любого потока даже во время прогона — новое ребро действует на значения, испущенные после него.

## Запуск

`Executor.run` — асинхронный генератор, отдающий выходы графа по мере их появления. Стартовому узлу нужен ровно один вход; туда подаётся начальное значение.

```python
ex = Executor(max_parallel=8)
async for out in ex.run(start, seed):
    ...
```

Чтобы остановиться раньше, выходите из цикла под `contextlib.aclosing(ex.run(start, seed))` — тогда ещё выполняющиеся узлы закроются чисто.

## Контроллеры

Контроллеры задаются для каждой задачи: `Task(input_controller=..., output_controller=..., execution_controller=...)`. Экземпляр контроллера — прототип: каждый узел, построенный из задачи, получает собственную рабочую копию. Аннотированные атрибуты — настройки как поля dataclass (только по имени), они переносятся в каждую копию; состояние, которое копия заводит для себя, живёт в `cached_property`, как стандартный `queues`.

Свой контроллер ввода наследует `InputControllerInterface` (или FIFO-`InputController`) и отвечает на `offer` и `poll` вердиктом: `Ready()`, `Wait()` или `WaitUntil(deadline)` в секундах монотонных часов. `collect` забирает из очередей входы одного такта:

```python
class Batching(InputController):
    size: int = 3

    def offer(self, delivery):
        self.queue(delivery.target.name).append(delivery.value)
        return self.poll()

    def poll(self):
        return Ready() if len(self.queue("batch")) >= self.size else Wait()

    def collect(self):
        queue = self.queue("batch")
        batch = list(queue)
        queue.clear()
        return Collected(data={"batch": batch})


class Consume(Task):
    def execute(self, batch):
        return GraphOutput(batch)


node = Consume(input_controller=Batching(size=10))()
```

Батчер по окну времени возвращал бы `WaitUntil(deadline)` из `poll`; когда дедлайн проходит, исполнитель опрашивает узел заново.

## Упорядоченная пара

`OrderedInputController` выпускает значения по возрастанию `value["idx"]` — значения здесь словари с ключом `"idx"` от 0, а вход у узла один. `OrderedOutputController` выпускает результаты в порядке тактов. В паре они позволяют тактам идти параллельно, а результатам выходить в порядке входа:

```python
worker = Worker(
    input_controller=OrderedInputController(),
    output_controller=OrderedOutputController(),
)()
```

## Изолированные узлы

`Task(isolated=True)` никогда не запускает два такта этого узла одновременно; входящие значения копятся в очереди, а следующий такт выдаётся, когда завершится текущий.

## Доставка одному получателю

`Downstream(value, shared=False)` доставляет значение ровно одному получателю вместо всех подключённых. Получателя выбирает политика испускающего узла — по умолчанию `ShortestQueue`. Своя политика наследует `RoutingPolicyInterface`, реализует `choose(targets, value)` и передаётся как `Task(policy=...)`.

## Исполнение нитями

`LocalExecutionController` (по умолчанию) запускает тело в текущем процессе и отдаёт тело циклу событий; все контроллеры поддерживают все четыре формы тела. `FiberExecutionController(threads=1, timeout=None, gateway=None, max_inflight=0)` вместо этого запускает тело как нить (fiber): каждый шаг выполняется в пуле рабочих потоков, а ожидания разрешаются на цикле событий, поэтому много медленных тел делят несколько потоков. Тело-нить может await-ить только четыре запроса ниже; голый `await` падает во время выполнения.

```python
class Crunch(Task):
    async def execute(self, value):
        data = await self.loop_io(lambda: fetch(value))  # async factory on the loop
        heavy = await self.offload(lambda: crunch(data))  # blocking call in the pool
        await self.sleep(0.1)  # timer without holding a worker
        reply = await self.remote("svc", "op", heavy)  # request through the gateway
        return GraphOutput(reply)


node = Crunch(execution_controller=FiberExecutionController(threads=4))()
```

`self.remote` требует шлюз: наследуйте `RemoteGateway`, реализуйте `async def submit(self, service, operation, payload)` и передайте его как `FiberExecutionController(gateway=...)`.

Полное описание модели — в [SPEC.ru.md](SPEC.ru.md).
