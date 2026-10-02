# actflow

[Русский](README.ru.md)

Version 0.2.0.

Task-graph execution with a two-level node model. The task body is a pure single-tick function that emits result values; the input and output controllers are a stateful bracket around it that decides readiness, ordering, and batching between ticks. Topology decides where a value goes, controllers decide when, tasks decide what, and the executor applies the state changes.

## Install

```
pip install -e .
```

## Quickstart

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

Prints `42`.

## Task body

Subclass `Task` and define `execute` with named parameters; input names are inferred from the signature, so a variadic body fails at node build. The body emits result objects: `Downstream(value, output="next")` goes on through the graph, `GraphOutput(value)` leaves it, and returning `None` emits nothing. Four body forms work:

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

## Building a graph

Calling a task instance builds a node; `>>` wires nodes, `["name"]` picks a port on either side:

```python
a = A()()
b = B()()

a >> b  # every output of a into every input of b
a >> b["left"]  # every output into one input
a["hot"] >> b  # one output into every input
a["hot"] >> b["left"]  # one output into one input
```

Input names are known from `execute`, so the right side of `>>` is validated loudly. Outputs are never declared: a body may emit any output name, and a value emitted on an unwired output is silently discarded. Duplicate edges collapse (targets are sets), self-loops are allowed (`node["retry"] >> node`), and edges may be added from any thread even mid-run — a new edge applies to values emitted after it.

## Running

`Executor.run` is an async generator that yields graph outputs as they happen. The start node needs exactly one input; the seed value is fed there.

```python
ex = Executor(max_parallel=8)
async for out in ex.run(start, seed):
    ...
```

To stop early, break out under `contextlib.aclosing(ex.run(start, seed))` so in-flight nodes are closed cleanly.

## Controllers

Controllers are passed per task: `Task(input_controller=..., output_controller=..., execution_controller=...)`. A controller instance is a prototype — every node built from the task takes its own working copy. Annotated attributes are dataclass settings (keyword-only) and travel into each copy; state a copy builds for itself lives in `cached_property`, like the standard `queues`.

A custom input controller subclasses `InputControllerInterface` (or the FIFO `InputController`) and answers `offer` and `poll` with a verdict: `Ready()`, `Wait()`, or `WaitUntil(deadline)` in monotonic seconds. `collect` dequeues the inputs for one tick:

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

A time-window batcher would return `WaitUntil(deadline)` from `poll`; the executor re-polls the node when the deadline passes.

## Ordered pair

`OrderedInputController` releases values in ascending `value["idx"]` order — values are dicts carrying `"idx"` starting from 0, and the node has a single input. `OrderedOutputController` releases results in tick order. Paired, they let ticks run in parallel while results leave in input order:

```python
worker = Worker(
    input_controller=OrderedInputController(),
    output_controller=OrderedOutputController(),
)()
```

## Isolated nodes

`Task(isolated=True)` never runs two ticks of that node concurrently; incoming values queue up, and the next tick is granted when the running one finishes.

## One-of routing

`Downstream(value, shared=False)` delivers the value to exactly one consumer instead of every wired one. The emitting node's policy picks the consumer — `ShortestQueue` by default. A custom policy subclasses `RoutingPolicyInterface`, implements `choose(targets, value)`, and is passed as `Task(policy=...)`.

## Fiber execution

`LocalExecutionController` (the default) runs the body in-process and hands the body to the event loop; every controller supports all four body forms. `FiberExecutionController(threads=1, timeout=None, gateway=None, max_inflight=0)` runs the body as a fiber instead: each step executes in a worker thread pool while waits resolve on the event loop, so many slow bodies share a few threads. A fiber body may await only the four requests below; a bare `await` fails at runtime.

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

`self.remote` needs a gateway: subclass `RemoteGateway`, implement `async def submit(self, service, operation, payload)`, and pass it as `FiberExecutionController(gateway=...)`.

The full model description lives in [SPEC.md](SPEC.md).
