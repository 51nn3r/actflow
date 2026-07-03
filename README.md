# actflow

[Русский](README.ru.md)

Task-graph execution with a two-level node model. The task body is a pure single-tick function; input/output controllers are a stateful bracket around it that handles readiness, ordering, and batching between ticks.

Full model description — [SPEC.md](SPEC.md).

## Structure

```
actflow/
  core.py       Packet, Collected, TaskResult, readiness verdicts, context vars
  control.py    InputController, OutputController, OrderedInputController
  node.py       Node, LinkRef — graph slot and async execution
  task.py       Task base class
  tasks.py      built-in tasks: Input, Terminal, Tap
  executor.py   SyncExecutor, AsyncExecutor, ExecutorHandle

examples/
  01_montecarlo_lasvegas.py   prover race, sibling cancellation
  02_redis_batcher.py         size-and-timeout batching, async body
  03_distributed_layers.py    distributed NN layers, order synchronizer
  04_naming_variants.py       naming spectrum: input_map, output_map, slot_map
```

## Run

```
cd actflow && pip install -e . && python examples/01_montecarlo_lasvegas.py
```

## API

### Task body

`execute` receives named parameters (matched to input slots by source label) and returns a routing dict or a list of `TaskResult`:

```python
class Double(Task):
    def execute(self, value) -> dict:
        return {"next": value * 2}
```

`None` as a key sends the value to the graph output:

```python
class Terminal(Task):
    def execute(self, value) -> dict:
        return {None: value}
```

Async bodies are transparent — the executor awaits them automatically:

```python
class Fetch(Task):
    async def execute(self, url) -> dict:
        data = await http_get(url)
        return {"next": data}
```

### Building a graph

`task()` creates a node; `>>` wires nodes; `["name"]` selects a named output socket:

```python
inp = Input()()
dbl = Double()()
end = Terminal()()

inp >> dbl >> end

SyncExecutor().run(inp, 21)   # → [42]
```

Named sockets and self-loops:

```python
class Retry(Task):
    def execute(self, item) -> dict:
        if done(item):
            return {"out": item}
        return {"retry": item}

node = Retry()()
node["retry"] >> node   # self-loop
node["out"] >> sink
```

### Source label

The source label is stamped on every outgoing packet and routes it to the matching input slot of the receiver. Set in `__init__`; defaults to the class name:

```python
w1 = Worker(label="w1")()
w2 = Worker(label="w2")()

class Collector(Task):
    def execute(self, w1, w2) -> dict:   # slot names match labels
        ...
```

For a simple graph (one node per class) no label config is needed — class names are unique by default.

### Naming controls

Slots are inferred from `execute`'s parameters; override or extend the naming when needed:

```python
Merge(in_labels=("left", "right"))  # declare slots (a **kwargs body can't infer them)
Two(input_map={"A": "a", "B": "b"})  # hop 1: edge label -> task slot
Split(output_map={"hi": "H", "lo": "L"})  # per-link outgoing label
Two(input_map={"A": "a"}, on_dropped=log)  # hook for an unmapped label
InputController(("a", "b"), slot_map={"a": "q1", "b": "q2"})  # hop 2: slot -> queue
```

See `examples/04_naming_variants.py` for the full spectrum.

### Task state and control

Inside `execute`, access node memory and executor control via `self`:

```python
class Counter(Task):
    def execute(self, value) -> dict:
        self.memory["n"] = self.memory.get("n", 0) + 1
        if self.memory["n"] >= 10:
            self.stop()
        return {"next": value}
```

Fan-out to the same link via `self.to()`:

```python
class Broadcast(Task):
    async def execute(self, items) -> list:
        return [self.to("out", item) for item in items]
```

### Custom controllers

Pass a custom input controller in `__init__` for batching, ordering, etc.:

```python
batcher = Batcher(input_controller=BatchInputController(("batch",)))()
collect = Collect(input_controller=OrderedInputController(("done",)))()
```

`OrderedInputController` is built in — delivers packets in strict ascending `idx` order and passes `mark={"idx": n}` to the output controller.