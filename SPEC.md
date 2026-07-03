# actflow — specification

[Русский](SPEC.ru.md)

A task-graph execution library. The graph describes which tasks run and where they send their results; the executor drives it, passing data between nodes. Supports sync and async mode, including remote and long-running tasks.

## 1. Core idea

A task only computes and returns a result — where to route it is determined by the graph's links, not by the task. The executor decides when to run each node. This gives it full control over ordering, parallelism, and stopping.

The model is close to functional programming and to computation graphs: immutable data flows along edges, nodes share no mutable state.

## 2. Two levels by time access

Everything in a node splits into two levels, divided by whether the passage of time is visible.

**Time-aware level — the body (execute).** A pure function of a single tick: receives values, returns values. It has no view of past or future, cannot span time between ticks, and holds no state.

**Timeless level — controllers.** They see all node queues and accumulated output; they hold state between ticks. Everything that needs memory across time belongs here: readiness, candidate selection, ordering, batch assembly, result decomposition.

## 3. Data packet

A packet — an envelope around a value — flows along edges.

```
packet = value + source label
```

**Value** — what the body works with. **Source label** — the slot address: where the packet lands in the receiving node. The packet is immutable: one result can be routed to multiple nodes without copying.

Metadata (sequence numbers for synchronization, etc.) does not travel with the packet — it lives in `Collected.mark`, produced by the input controller during `collect()` and consumed by the output controller during `emit()` within the same tick (see section 6).

## 4. Source label and routing

The destination slot is determined by the **source label**, not by the order of arrival.

By default, a node stamps its result with its own source label: a node whose task has `label="A"` produces data labeled `A`. A receiving node with `execute(self, A, C)` routes incoming `A` to the `A` slot and `C` to the `C` slot.

**Label resolution** (first non-empty wins): `out_labels[0]` class var → `label` constructor arg → class name. For a simple graph with one node per class, the class name is unique and no explicit label config is needed.

If two nodes of the same class send to the same collector, name them:

```python
w1 = Worker(label="w1")()
w2 = Worker(label="w2")()

class Collector(Task):
    def execute(self, w1, w2) -> dict: ...
```

By default, an unknown incoming label is bound to the first free slot (and remembered, so the same label always lands there); a known label goes straight to its queue.

**Slot inference.** Slots come from the `execute` parameter names (minus `self`/`ctx`), or from `in_labels`. A variadic body (`*args`/`**kwargs`) has no fixed arity — slots can't be inferred, and building the node fails loud with a `TypeError` unless `in_labels` is set.

**Name translation.** Three namespaces meet at a node: the **edge label** (on the wire), the **task slot** (`execute`'s parameter), and the **controller queue** (the controller's internal name). By default all three coincide, so nothing needs configuring. When they differ, two independent hops bridge them:

- **Hop 1, on the node** — `input_map` (edge → slot) renames an incoming label to a task slot. Without a map, the label falls through to the default routing above; with a map, a label absent from it is dropped and passed to `on_dropped(packet)` (a no-op by default). Map targets are validated against the task's slots at build time.
- **Hop 2, in the controller** — `slot_map` (slot → queue); see section 6.

On the output side, `output_map` (link name → edge label) sets a per-link outgoing label (default: the node's source label). One node can thus send distinct labels down different links to the same receiver, which the receiver's `input_map` then splits into separate slots.

## 5. Input queues and candidate selection

Each node input is a named slot with a FIFO queue. When multiple packets arrive at the same slot, the default is FIFO, but the selection rule is replaceable — it's part of the input controller.

## 6. Paired controllers — an invertible bracket around the body

The input and output controllers act as an invertible pair. The input applies a transformation; the output applies the inverse. The body in between doesn't know it's been wrapped. The node is the facade over both: the executor talks to the node (`offer` / `poll` / `collect` / `dispatch`), never to the controllers directly.

```
collect → body: execute → emit
```

**Input controller** (timeless): decides readiness, selects from the queue, assembles inputs into what the body will see. `collect()` returns `Collected(data, mark)`:

- `data` — the dict of named inputs passed to `execute(**data)`
- `mark` — opaque metadata the input controller wants the output controller to receive (e.g., sequence index for reordering). Travels as a local variable through the tick; safe under concurrent execution.

**Output controller** (timeless): receives the body's result and `mark` in `emit(results, mark)` and turns it into `(value, target, label)` triples. Source-label stamping is the **node's** job — it applies the default label and `output_map`; the default `OutputController` just passes results through.

**Hop 2 — controller queues** (`slot_map`, slot → queue): a controller carries its own internal queue names, so one controller can be reused across tasks with different slot names. `slot_map` renames task slots to the controller's queues 1-to-1 (default: identity); `collect()` maps them back, so the body still sees its own slot names.

Examples of paired transforms:

- **Synchronization.** `OrderedInputController` holds out-of-order arrivals and releases them in strict `idx` order, passing `mark={"idx": n}` to the output side (example 03).
- **Pack–unpack.** Input folds multiple packets into one batch; body processes the whole object; output splits the result back.
- **Normalize–denormalize.** Input scales/reshapes values to working form; body computes; output converts back.

Ordinary nodes don't need custom controllers — defaults are provided: `InputController` (FIFO, ready when all slots are non-empty) and `OutputController` (pass-through; the node stamps the source label).

## 7. Readiness and wakeup

Data is received through the node, not directly into the queue. When a packet arrives, the input controller immediately signals whether a run is ready. The answer is one of three:

```
Ready()          — run now
Wait()           — not ready, wait for new data
WaitUntil(T)     — not ready, but wake no later than T (batch deadline)
```

Only the executor sleeps and wakes. It collects all deadlines T and sleeps until "a node completes or the nearest deadline T". The input controller has no thread or timer — it only names a deadline. This is how time-based batching works without a dedicated thread and without busy-waiting.

## 8. Body: sync, async, remote

The body is `execute`. It may be:

- **sync** — a plain function; the executor waits for it
- **async** — a coroutine; long-running or remote work is expressed via `await`

`Node.run(handle)` is always an async coroutine. `SyncExecutor` wraps it in `asyncio.run()`; `AsyncExecutor` awaits it directly with a parallelism cap. The executor doesn't care whether the body computes locally or waits on the network.

Node memory and executor control are available inside `execute` via context variables bound for the duration of each tick:

```python
self.memory       # per-node dict, persists across ticks
self.stop()       # graceful stop: current ticks finish, no new ones start
self.snapshot()   # executor state dict
self.to(name, v)  # create a TaskResult for fan-out
```

## 9. Task result

`execute` returns one of:

- `dict` — `{"link_name": value, ...}`; key `None` sends to graph output
- `list[TaskResult]` — for fan-out or explicit source label override
- `None` — no output (fire-and-forget)

`TaskResult(value, target_node, label)` — explicit target node and optional label override. Created via `self.to(link_name, value, label=None)`.

## 10. Executor

One loop, two modes:

- delivers results of completed nodes to recipient queues (via their input controller), receives readiness responses
- launches all Ready nodes
- sleeps until "a node completes or the nearest deadline T"
- wakes up and repeats

**SyncExecutor** — `asyncio.run(node.run(handle))` per node; bodies run sequentially.
**AsyncExecutor** — `await node.run(handle)` via `asyncio.ensure_future`; all ready bodies run concurrently, bounded by `max_parallel`.

Ready nodes are held in an insertion-ordered set (`ReadyQueue`); deadline-parked nodes in a `WaitSet`. The task-facing control handle is `ExecutorHandle` (`self.stop()` / `self.snapshot()`).

## 11. Control through the graph

Control code is a regular graph node. `self.stop()` and `self.snapshot()` are available inside any `execute` body. Stopping a sibling branch, a watchdog, logging — all expressed as nodes, without external listeners.