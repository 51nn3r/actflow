"""actflow example 05: an AI task graph over an async ML backend.

Each node submits one unit of work to an async backend — `submit(kind, payload)` —
and its async body completes WHEN THE RESPONSE ARRIVES. This mirrors the langio
conductor: nodes drop work on a queue and wake on the reply. Because the executor
awaits many bodies concurrently, a graph (not just a linear chain) falls out for free:
here two encodes run in parallel, a merge node compares them, and a final node shapes
the response — "response forms as tasks".

The backend here is an in-file fake so the example runs standalone; in langio it is a
ConductorGateway backed by Redis/celery. Run: python examples/05_conductor_graph.py
"""
import asyncio
import contextvars
import math

from actflow import Task, AsyncExecutor


# The backend is ambient for the duration of a run — same pattern actflow uses for
# node memory and the executor handle. A runner sets it; tasks read it.
_backend: contextvars.ContextVar = contextvars.ContextVar("_backend")


def current_backend() -> "MLBackend":
    return _backend.get()


class MLBackend:
    """Async request/reply ML backend: submit(kind, payload) -> result dict."""

    async def submit(self, kind: str, payload: dict) -> dict:
        raise NotImplementedError


class FakeMLBackend(MLBackend):
    """Stand-in for the conductor: toy deterministic embeddings after a small delay."""

    async def submit(self, kind: str, payload: dict) -> dict:
        await asyncio.sleep(0.02)  # network + model latency
        if kind == "embed":
            return {"vector": _toy_embed(payload["text"])}

        raise ValueError(f"unknown kind {kind!r}")


def _toy_embed(text: str) -> list[float]:
    """A cheap bag-of-letters vector so cosine actually reflects text overlap."""
    buckets = [0.0] * 8
    for ch in text.lower():
        if ch.isalpha():
            buckets[ord(ch) % 8] += 1.0

    return buckets


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if not na or not nb:
        return 0.0

    return dot / (na * nb)


# --- graph nodes ---------------------------------------------------------------

class Fork(Task):
    """Split the (query, doc) pair to the two encoders."""

    def execute(self, pair: tuple) -> dict:
        return {"q": pair[0], "d": pair[1]}


class Encode(Task):
    """Submit one text for embedding and forward the vector.
    The source label (set via label=) routes the vector into the right merge slot."""

    async def execute(self, text: str) -> dict:
        result = await current_backend().submit("embed", {"text": text})
        return {"next": result["vector"]}


class Compare(Task):
    """Merge node: two vectors in, cosine similarity out."""

    def execute(self, query_vec: list, doc_vec: list) -> dict:
        return {"next": _cosine(query_vec, doc_vec)}


class ShapeResponse(Task):
    """Response-form-as-a-task: raw score -> client-facing shape, emitted as graph output."""

    def execute(self, score: float) -> dict:
        return {None: {"match": score >= 0.95, "score": round(score, 4)}}


def build() -> Fork:
    fork = Fork()()
    encode_query = Encode(label="query_vec")()
    encode_doc = Encode(label="doc_vec")()
    compare = Compare()()
    shape = ShapeResponse()()

    fork["q"] >> encode_query
    fork["d"] >> encode_doc
    encode_query >> compare  # source label "query_vec" lands in the query_vec slot
    encode_doc >> compare    # source label "doc_vec" lands in the doc_vec slot
    compare >> shape

    return fork


async def main() -> None:
    _backend.set(FakeMLBackend())
    print("=== AI graph over an async ML backend (2 concurrent encodes -> compare -> shape) ===")
    for query, doc in [("the cat sat on the mat", "the cat sat on the mat"),
                       ("the cat sat on the mat", "quantum chromodynamics lecture")]:
        result = await AsyncExecutor().run(build(), (query, doc))
        print(f"query={query!r} doc={doc!r} -> {result}")


if __name__ == "__main__":
    asyncio.run(main())
