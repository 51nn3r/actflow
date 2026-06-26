# actflow

[Русский](README.ru.md)

Task-graph execution with a two-level node model. The task body is a pure single-tick function; input/output controllers are a stateful bracket around it that handles readiness, ordering, and batching between ticks.

Full model description — [SPEC.md](SPEC.md).

## Structure

```
actflow/
  core.py       data packet, addressed result, readiness verdicts
  control.py    input/output controllers (default implementations)
  node.py       graph node and body invocation context
  task.py       base task class
  tasks.py      built-in tasks: Input, Terminal, Tap
  executor.py   sync and async executors, control handles

examples/
  01_montecarlo_lasvegas.py   prover race, sibling cancellation
  02_redis_batcher.py         size-and-timeout batching, async body
  03_distributed_layers.py    distributed NN layers, order synchronizer
```

## Run

```
PYTHONPATH=. python examples/01_montecarlo_lasvegas.py
```

## API

A task computes and routes its result by link name:

```python
class Double(Task):
    def execute(self, inputs, ctx):
        x = next(iter(inputs.values()))
        return [ctx.to("next", x * 2)]
```

A graph is built from nodes with named links (self-links create cycles):

```python
inp = Input()()
dbl = Double()()
end = Terminal()()
inp.link("next", dbl)
dbl.link("next", end)

SyncExecutor().run(inp, 21)   # → [42]
```

The body may be async — remote or long-running work is transparent:

```python
class Remote(Task):
    async def execute(self, inputs, ctx):
        data = await redis_call(...)
        return [ctx.to("next", data)]
```

Custom input behavior (batching, ordering) — replace the input controller:

```python
node.input = BatchInput(("batch",))
```

Control is expressed as graph nodes via `ctx.control`:

```python
Tap(lambda ctx, v: ctx.control.stop())
Tap(lambda ctx, v: print(ctx.control.snapshot()))
```