# actflow - specification

[Русский](SPEC.ru.md)

A task-graph execution library. The wiring describes where results go, tasks describe what one tick computes, controllers decide when things happen, and the executor streams values through the graph as they are produced. This document describes actflow 0.2.0.

## 1. Core idea

```
Topology decides where.
Controllers decide when.
Tasks decide what.
The executor applies state changes.
```

A task only computes: it receives values and emits results, never knowing who consumes them - routing belongs to the wiring. The body is a pure function of one tick and holds no state between runs; everything that spans time (readiness, ordering, batching, delivery) lives in controllers. The executor alone mutates run state: it is single-threaded, and control switches only at await points, never by preemption.

## 2. Tasks and bodies

Subclass `Task` and define `execute` with named parameters. The parameter names are the node's inputs, inferred from the signature when the class is created; a variadic body (`*args` / `**kwargs`) has no fixed arity and fails loud at node build.

Every execution controller accepts four body forms (the interface normalizes them into one stream before driving):

```python
class Plain(Task):
    def execute(self, value):
        return Downstream(value + 1)


class Coro(Task):
    async def execute(self, value):
        return GraphOutput(await lookup(value))


class Gen(Task):
    def execute(self, value):
        for part in value:
            yield Downstream(part)


class AsyncGen(Task):
    async def execute(self, value):
        async for part in stream(value):
            yield GraphOutput(part)
```

The body emits result objects:

- `Downstream(value, output="next", shared=True)` goes on through the graph, on the named output.
- `GraphOutput(value)` leaves the graph and is yielded to the caller of `Executor.run`.
- Returning `None`, or a generator that yields nothing, emits nothing.

Anything else raises `TypeError` at dispatch. A shared `Downstream` hands every consumer the same reference; copying is the task's business.

Two tiny tasks ship with the library: `Input` forwards its value on `next`, `Tap` runs a side-effect action and then forwards.

## 3. Topology

A `Task` instance is configuration; calling it builds a node, and every call builds a fresh one with its own controller copies:

```python
node = MyTask()()
```

Wire with `>>`. `node["name"]` is a port; input or output is decided by its side of the operator:

```python
a >> b
a >> b["inp"]
a["out"] >> b
a["out"] >> b["inp"]
```

`a >> b` connects every output of `a` to every input of `b`; naming a port narrows that side to the one name. `>>` returns the target node, so chains read left to right: `a >> b >> c`.

Validation is asymmetric. Inputs are known from the `execute` signature, so `a >> b["typo"]` fails at wire time. Outputs are never declared: a node may emit any name at any moment, the left side cannot be checked, and a value emitted on an output nobody wired is silently discarded. Targets are sets, so duplicate edges collapse: wiring the same pair twice, or a wildcard edge next to a named one into the same input, still delivers once.

The graph may grow while it runs: `wire` takes the node's lock and may be called from any thread, and addressing is lazy - a new edge applies from the next emitted value. `node.coverage()` returns the node and everything reachable from it along the wiring. `node.clear()` resets that coverage to initial controller state (wiring stays); `node.clone()` builds an independent twin of it - same tasks and wiring, fresh state, prototype-held resources such as a fiber pool stay shared.

## 4. Controllers are prototypes

A node has three controllers and a routing policy:

- input controller - holds arrived values, decides when the body may run
- output controller - owns delivery: where a produced value goes, how, and when
- execution controller - runs the body, decides where it executes
- routing policy - picks the one consumer of a non-shared value

All four derive from `Controller`, and their subclasses become keyword-only dataclasses automatically, so an annotated attribute is a setting. The instance passed to `Task(...)` is a prototype: every node built from the task takes its own working copy via `replace`. Keep settings in dataclass fields and per-node state in `cached_property`, and the copy starts clean.

```python
Task(
    input_controller=None,
    output_controller=None,
    execution_controller=None,
    isolated=False,
    policy=None,
)
```

## 5. Input side: verdicts

An input controller implements `offer(delivery)`, `poll()` and `collect()` over the standard `queues` dict (one list per input) and `queue(name)`. `offer` receives a `Delivery` - the value is `delivery.value`, the input it is addressed to is `delivery.target.name` - and answers with a verdict; `poll` re-answers without new data:

- `Ready()` - run now.
- `Wait()` - wake only when new data arrives.
- `WaitUntil(deadline)` - wake no later than the deadline (monotonic seconds).

The controller never sleeps and owns no timer: it names a deadline, and the executor parks the node and wakes it. Time-based batching costs no thread and no busy-wait.

`collect()` dequeues one tick's inputs and returns `Collected(data, metadata)`: `data` is passed as `execute(**data)`, `metadata` rides to the output controller of the same tick. The executor collects atomically with the `Ready` verdict, so a queued value is never granted twice.

Defaults: `InputController` is FIFO and ready when every input queue is non-empty. `OrderedInputController` feeds a single-input node its values in `value["idx"]` order, contiguous from 0 - a missing index stalls everything after it - and stamps `metadata={"idx": n}`.

## 6. Output side and routing

The output controller is called once per produced value: `dispatch(result, metadata)` returns the deliveries the executor applies, and an empty list means the value is held back. The method is async, so a controller may wait for its turn before letting a value out. `close_tick(metadata)` runs once when the tick's body is exhausted.

The default `OutputController` releases everything at once. A shared `Downstream` goes to every consumer wired to that output; `Downstream(..., shared=False)` goes to exactly one, picked by the emitting node's policy. The default policy `ShortestQueue` picks the target whose input queue is shortest; custom policies subclass `RoutingPolicyInterface` and implement `choose(targets, value)`. Queues only build up at busy consumers (an isolated or slow node): between fast ones every queue reads zero at choose time, so one target stably receives everything.

`OrderedOutputController` restores input order to a node that ticks in parallel: each dispatch waits until `metadata["idx"]` matches the turn, and `close_tick` advances the turn, so a tick that emitted nothing still passes it on. Pair it with `OrderedInputController`, which stamps the idx. A waiting dispatch holds its executor slot: idx values stamped out of grant order can stall the run.

## 7. Execution controllers

`LocalExecutionController` (the default) hands the body stream to the event loop as is. A custom controller subclasses `ExecutionControllerInterface` and implements one method, `drive(stream)`: where the normalized stream executes; the four body forms are sorted out by the interface before it.

`FiberExecutionController(threads=1, timeout=None, gateway=None, max_inflight=0)` runs the body as a fiber: steps execute in a pool of `threads` worker threads, and every await returns control to the central loop. The body may await only what `self` offers:

- `self.sleep(delay)` - timer; holds no worker.
- `self.loop_io(factory)` - run an async factory on the central loop (e.g. aiohttp).
- `self.offload(fn)` - run a blocking callable in the step pool, competing for the same worker slots.
- `self.remote(service, operation, payload)` - send to the gateway and await the reply.

A bare `await` on anything else fails at runtime, not at build. `timeout` is a deadline on the whole tick once its in-flight slot is held: time queued for the slot is excluded, time between results is not. It is enforced at the controller's own await points - a step running in a thread cannot be interrupted, so an overrun surfaces when that step ends. An overrun that falls entirely inside the body's final step is not punished: the work is already done, the results stand. Cancellation is thrown into the body, so a synchronous `finally` runs; a `finally` that awaits is not driven further. The raised `TimeoutError` then ends the run like any body error. `max_inflight > 0` caps the controller's concurrent fibers, per event loop.

All node copies of one prototype share one pool and one `max_inflight` cap; pass `pool=` to share the pool wider. The creator shuts a pool down, and an orphaned pool is joined at interpreter exit.

`RemoteGateway` is the transport behind `self.remote`: subclass it, implement `async submit(service, operation, payload)`, and pass it as `FiberExecutionController(gateway=...)`.

## 8. Executor

```python
ex = Executor(max_parallel=8)
async for out in ex.run(start_node, seed):
    ...
```

`run` is an async generator: it seeds the start node, which needs exactly one input, then yields each `GraphOutput` value as it leaves the graph. To stop at the first result, break out under `contextlib.aclosing`.

The loop waits on each running node's next emitted value, not on the node finishing, so a generator body's first value moves downstream while the body still runs. The wait carries a timeout from the nearest `WaitUntil` deadline, and due nodes are re-polled. `max_parallel` is a semaphore held for the whole tick. `Scheduler`, the executor's base, alone holds the run state: who may run now, who is parked until when. One Executor serves one run at a time; a second concurrent `run` raises. Controller state lives on the nodes and survives runs - rerun from scratch via `node.clear()`, or run an independent copy via `node.clone()` and `ex.clone()`. Call `clear()` between runs only: clearing a coverage an Executor is running breaks that run.

`isolated=True` on a task means never two concurrent ticks of that node: while one tick runs, further grants are suppressed, and the queued values are granted when it finishes.

An error in a body ends the run: in-flight ticks are cancelled and the exception propagates to the caller of `run`.

## 9. Breaking changes vs 0.1.x

- Source labels are gone: `label`, `in_labels` / `out_labels`, `input_map` / `output_map`, `slot_map`, `on_dropped`. Where several edges met a collector by label auto-binding, wire explicitly: `a >> collector["a"]`.
- The result protocol changed: dict returns (`{"next": v}`, `{None: v}`), `TaskResult` and `self.to()` are gone. Emit `Downstream` / `GraphOutput`, or yield them from a generator body. The `Terminal` task went with `TaskResult`; return `GraphOutput` instead.
- The controller surface moved with it: `offer` receives a `Delivery` and `Packet` is gone, `Collected.mark` is now `metadata`, and the output side is async `dispatch(result, metadata)` per value plus `close_tick`, replacing `emit(results, mark)`.
- `a >> b` now means every output into every input, not `next` into the single input.
- `AsyncExecutor` is renamed `Executor`; `SyncExecutor` is gone. `run()` now returns an async generator, not a list: iterate it inside a coroutine, e.g. `asyncio.run(main())` where `main` does `[x async for x in ex.run(start, seed)]` - a bare `asyncio.run(ex.run(...))` does not work. `fiber_workers` (now named `threads`) / `gateway` / `max_inflight` moved off the executor onto `FiberExecutionController`.
- `ExecutorHandle`, `task.stop()`, `task.snapshot()`, `task.memory` are gone: a body cannot stop the run or read executor state.
- `Task.links` is gone: a body does not read the topology.
- The `actflow.fiber` module is gone; import everything from the `actflow` package root.
- Controllers passed to `Task.__init__` are prototypes now: two nodes built from one task no longer share controller state.
- A custom execution controller now implements `drive(stream)` instead of overriding `run`; `run` is a concrete normalizer on the interface.
- `LinkRef` is renamed `NodePort`; `node["name"]` returns one.
- `ExecutionRuntime` is gone: `FiberExecutionController` owns its pool; pass `pool=` to share it.
