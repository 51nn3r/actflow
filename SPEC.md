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
packet = value + type label
```

**Value** — what the body works with. **Type label** — the slot address: where the packet lands in the receiving node. The packet is immutable: one result can be routed to multiple nodes without copying. If a task needs a mutable copy, it copies explicitly (important for tensors: no redundant allocations).

Metadata (sequence numbers for synchronization, etc.) does not travel with the packet — it lives in the controller state of the node that issued it (see section 6).

## 4. Type-based routing

The destination slot is determined by the **type label**, not by the order of arrival.

By default, a node stamps its result with its own type label: node `a` produces data labeled `A`. Node `b`, declared as accepting `[A, C]`, routes incoming `A` to the `A` queue and incoming `C` to the `C` queue.

Slots are bound to types when inserted into the graph: in declaration order, one type per slot; explicit binding is also possible. If node `d` wants to send where `A` is expected, it **changes the label** to `A` before sending — changing the label means selecting a slot.

## 5. Input queues and candidate selection

Each node input is a named queue (by type label). When multiple packets arrive at the same slot, the question is: which to take next. Default is FIFO, but the selection rule is replaceable — it's part of the input controller.

## 6. Paired controllers — an invertible bracket around the body

The input and output controllers act as an invertible pair. The input applies a transformation; the output applies the inverse. The body in between doesn't know it's been wrapped.

```
input: transform → body: compute → output: inverse transform
```

**Input controller** (timeless): decides readiness, selects from the queue, sets ordering, assembles inputs into what the body will see (e.g., folds time steps into a batch).

**Output controller** (timeless): decomposes the body's result back into output queues (splits a batch into steps), stamps labels and addresses.

Input and output coordinate via a **receipt**: while passing packets to the body, the input returns "what to remember" (e.g., a sequence number); the executor carries it to the output and hands it in together with the body's result. The receipt travels with the task and survives network transit — unlike shared memory, it works in distributed mode.

Examples of paired transforms:

- **Synchronization.** Input records the sequence number; output reattaches it and holds results until they can be released in order. Restores ordering disrupted by uneven processing speed.
- **Pack–unpack.** Input folds multiple packets into one batch; body processes the whole object; output splits the result back. One network pass per batch instead of N per item.
- **Normalize–denormalize.** Input scales/reshapes values to working form; body computes; output converts back.

Ordinary nodes don't need custom controllers — defaults are provided: input is FIFO ("one packet per slot, ready when all slots are non-empty"); output stamps with the node's type label and dispatches along links.

## 7. Readiness and wakeup

Data is received through the node, not directly into the queue. When a packet arrives, the input controller immediately signals whether a run is ready. The answer is one of three:

```
READY           — run now
WAIT            — not ready, wait for new data
WAIT_UNTIL(T)   — not ready, but wake no later than T (batch deadline)
```

Only the executor sleeps and wakes. It collects all deadlines T and sleeps until "a node completes or the nearest deadline T". The input controller has no thread or timer — it only names a deadline. This is how time-based batching works without a dedicated thread and without busy-waiting.

## 8. Body: sync, async, remote

The body is `execute`. It can be:

- **sync** — a plain function; the executor waits for it in place
- **async** — a coroutine; long-running or remote work (redis, another node) is expressed via `await`, the executor sleeps and handles other nodes in the meantime

The executor doesn't care whether the body computes locally or waits on the network: in async mode it called `execute` on all ready nodes and slept; the first completed coroutine woke it up. No separate resumption mechanism needed.

## 9. Task result

The body returns a list of addressed results:

```
TaskResult = (data, target node)
```

Typically one result and one target; a list is needed for branching and fan-out. The slot within the target is selected by type label (section 4). Graph output is handled by a terminal task, not a special result type.

## 10. Executor

One loop, two modes:

- delivers results of completed nodes to recipient queues (via their input controller), receives readiness responses
- launches all READY nodes
- sleeps until "a node completes or the nearest deadline T"
- wakes up and repeats

**SyncExecutor** waits for each body in place (ready nodes run sequentially). **AsyncExecutor** launches all ready bodies at once and sleeps on their completion, with a parallelism cap; long-running and remote bodies are transparent.

The sleeper is always one — the executor — on `asyncio.wait` with timeout (async) or `time.sleep` (sync). No busy-waiting: with sub-second deadlines a spinlock would burn a CPU core and in async mode would starve waiting coroutines.

## 11. Control through the graph

Control code is a regular graph node. Through the context it accesses executor handles: stop (graceful: current runs finish, no new ones start) and a state snapshot for logging. Stopping a sibling branch, a watchdog, logging — all expressed as nodes, without external listeners.