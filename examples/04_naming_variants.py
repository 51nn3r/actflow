"""actflow: one task shape across the whole naming spectrum.

From full auto (slots inferred) to full custom: slots, source label,
input_map, output_map, a custom controller (slot_map), on_dropped.
Tasks with 1/2/3 inputs are defined here too. Run: python examples/04_naming_variants.py
"""
import asyncio

from actflow import Task, InputController, AsyncExecutor, Packet


# --- tasks with different input counts (slots inferred from execute params) ---

class One(Task):
    """1 input; slot inferred as 'value'."""

    def execute(self, value) -> dict:
        return {None: value}


class Two(Task):
    """2 inputs; slots inferred as 'a', 'b'."""

    def execute(self, a, b) -> dict:
        return {None: (a, b)}


class Three(Task):
    """3 inputs; slots inferred as 'x', 'y', 'z'."""

    def execute(self, x, y, z) -> dict:
        return {None: (x, y, z)}


class Merge(Task):
    """**kwargs body: slots cannot be inferred, declare them via in_labels."""

    def execute(self, **parts) -> dict:
        return {None: parts}


class Split(Task):
    """Sends to two links; output_map gives each link its own edge label."""

    def execute(self, value) -> dict:
        return {"hi": value + 100, "lo": value - 100}


def variants() -> None:
    """One task, built across the whole spectrum of naming control."""
    print("=== naming variants: from auto to full custom ===")

    # 1. Full auto: slots from execute params, source label = class name.
    n = Two()()
    print("1 auto: slots", n.input_controller.labels, "| label", n.source_label)
    assert n.input_controller.labels == ("a", "b")
    assert n.source_label == "Two"

    # A varying input count is inferred from the signature on its own.
    assert One()().input_controller.labels == ("value",)
    assert Three()().input_controller.labels == ("x", "y", "z")

    # 2. Source label via label.
    print("2 label:", Two(label="join")().source_label)
    assert Two(label="join")().source_label == "join"

    # 3. out_labels[0] overrides label.
    print("3 out_labels:", Two(out_labels=("JOIN",), label="join")().source_label)
    assert Two(out_labels=("JOIN",), label="join")().source_label == "JOIN"

    # 4. Slots declared explicitly via in_labels (**kwargs body).
    n = Merge(in_labels=("left", "right"))()
    print("4 in_labels: slots", n.input_controller.labels)
    assert n.input_controller.labels == ("left", "right")

    # 5. input_map: incoming source labels -> my slots (hop 1).
    n = Two(input_map={"A": "a", "B": "b"})()
    n.offer(Packet(1, "A"))
    n.offer(Packet(2, "B"))
    data = n.collect().data
    print("5 input_map: A->a, B->b =>", data)
    assert data == {"a": 1, "b": 2}

    # 6. output_map: per-link outgoing label (default = source label).
    s = Split(output_map={"hi": "H", "lo": "L"})()
    s.links["hi"] = s.links["lo"] = One()()
    labels = {value: label for value, _, label in s.dispatch({"hi": 1, "lo": 2}, None)}
    print("6 output_map: hi->H, lo->L =>", labels)
    assert labels == {1: "H", 2: "L"}

    # 7. Custom controller: its own queue names (slot_map, hop 2).
    ic = InputController(("a", "b"), slot_map={"a": "q1", "b": "q2"})
    n = Two(input_controller=ic)()
    n.offer(Packet(1, "a"))
    n.offer(Packet(2, "b"))
    print("7 slot_map: queues", set(ic.queues), "=> collect", n.collect().data)
    assert set(ic.queues) == {"q1", "q2"}

    # 8. on_dropped: hook for an unmapped source label.
    dropped = []
    n = Two(input_map={"A": "a", "B": "b"}, on_dropped=dropped.append)()
    n.offer(Packet(1, "A"))
    n.offer(Packet(99, "UNKNOWN"))
    print("8 on_dropped: dropped", [p.value for p in dropped])
    assert [p.value for p in dropped] == [99]

    # 9. Fail-loud: input_map target outside the slots -> error at build.
    try:
        Two(input_map={"A": "zzz"})()
        assert False, "expected ValueError"
    except ValueError:
        print("9 validation: input_map={'A':'zzz'} -> ValueError")

    # 10. Fail-loud: variadic body without in_labels -> slots cannot be inferred.
    try:
        Merge()()
        assert False, "expected TypeError"
    except TypeError:
        print("10 variadic: Merge() without in_labels -> TypeError")


async def run_graph() -> None:
    """Full custom through the executor: output_map -> input_map -> slot_map.
    Source fans out to one receiver over two links; output_map tags them, input_map splits them into slots."""
    print("\n=== full custom in action ===")

    class Source(Task):
        """Fans the trigger out to two links."""

        def execute(self, trigger) -> dict:
            return {"left": 10, "right": 20}

    class Combine(Task):
        """Two inputs on a controller with slot_map; input_map splits labels into slots."""

        def execute(self, a, b) -> dict:
            return {None: a + b}

    source = Source(output_map={"left": "L", "right": "R"})()
    combine = Combine(
        input_map={"L": "a", "R": "b"},
        input_controller=InputController(("a", "b"), slot_map={"a": "q1", "b": "q2"}),
    )()
    source["left"] >> combine
    source["right"] >> combine

    ex = AsyncExecutor()
    result = await ex.run(source, "go")
    print("source fan-out -> combine(a=10, b=20) -> sum:", result)
    assert result == [30], result
    print("snapshot:", ex.snapshot())


async def main() -> None:
    variants()
    await run_graph()
    print("\nall variants ok")


if __name__ == "__main__":
    asyncio.run(main())
