"""Graph behaviour: input readiness, wiring, ordering.

Through the public API only: nodes, `>>`, sockets, graph output.
"""
from actflow import Downstream, GraphOutput, OrderedInputController, Task


class Fork(Task):
    def execute(self, pair) -> list:
        yield Downstream(pair[0], output="left")
        yield Downstream(pair[1], output="right")


class End(Task):
    def execute(self, value) -> GraphOutput:
        return GraphOutput(value)


def test_node_runs_only_when_every_input_is_filled(walk):
    """A two-input node does not run while only one input has a value."""
    calls = []

    class Pair(Task):
        def execute(self, a, b) -> GraphOutput:
            calls.append((a, b))
            return GraphOutput((a, b))

    fork, pair = Fork()(), Pair()()
    fork["left"] >> pair["a"]
    fork["right"] >> pair["b"]

    assert walk(fork, (1, 2)) == [(1, 2)]
    assert calls == [(1, 2)], "exactly one run, with both values"


def test_values_on_one_input_arrive_in_order(walk):
    """Several values into one input are taken in arrival order."""

    class Emit(Task):
        def execute(self, value):
            for i in (1, 2, 3):
                yield Downstream(i)

    class Collect(Task):
        def execute(self, value) -> GraphOutput | None:
            got = self.memory.setdefault("got", [])
            got.append(value)
            return GraphOutput(list(got)) if len(got) == 3 else None

    emit, collect = Emit()(), Collect()()
    emit >> collect

    assert walk(emit, None) == [[1, 2, 3]]


def test_memory_survives_between_ticks(walk):
    """`self.memory` belongs to the node and outlives a tick."""

    class Counter(Task):
        def execute(self, n) -> Downstream | GraphOutput:
            self.memory["seen"] = self.memory.get("seen", 0) + 1
            if self.memory["seen"] == 3:
                return GraphOutput(self.memory["seen"])

            return Downstream(n, output="again")

    node = Counter()()
    node["again"] >> node["n"]

    assert walk(node, None) == [3]


def test_self_loop(walk):
    """A node can be wired to itself."""

    class Countdown(Task):
        def execute(self, n) -> Downstream | GraphOutput:
            return GraphOutput("done") if n <= 0 else Downstream(n - 1, output="again")

    node = Countdown()()
    node["again"] >> node["n"]

    assert walk(node, 3) == ["done"]


def test_one_output_feeds_two_nodes(walk):
    """Fan-out: one output, several receivers."""
    src = Fork()()
    src["left"] >> End()()
    src["left"] >> End()()

    assert walk(src, (7, 0)) == [7, 7]


def test_same_edge_twice_delivers_once(walk):
    """Wiring the same edge twice does not double the delivery."""
    src, end = Fork()(), End()()
    src["left"] >> end
    src["left"] >> end

    assert walk(src, (7, 0)) == [7]


def test_wildcard_output_takes_every_name(walk):
    """`a >> b` takes every output of a, including names not seen yet."""
    src, end = Fork()(), End()()
    src >> end

    assert sorted(walk(src, (1, 2))) == [1, 2]


def test_ordered_input_releases_by_index(walk):
    """`OrderedInputController` holds back what arrived out of order."""
    seen = []

    class Spread(Task):
        def execute(self, value):
            for i in (2, 0, 1):
                yield Downstream({"idx": i})

    class Ordered(Task):
        def execute(self, item) -> None:
            seen.append(item["idx"])

    spread = Spread()()
    ordered = Ordered(input_controller=OrderedInputController())()
    spread >> ordered

    walk(spread, None)
    assert seen == [0, 1, 2]