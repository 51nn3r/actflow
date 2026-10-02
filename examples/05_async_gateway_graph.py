"""actflow example 05: a task graph over an async ML gateway.

Each node submits work to a request/reply gateway and its async body resumes
when the reply arrives. The executor awaits many bodies at once, so the two
encodes run in parallel, a merge node compares the vectors, and a last node
shapes the client response. Bodies are plain coroutines (example 08 runs the
same gateway request/reply pattern as a fiber); the gateway is an in-file fake, so this runs standalone.
"""

import asyncio
import math

from actflow import Downstream, Executor, GraphOutput, Task

REPLY_DELAY = 0.02
MATCH_THRESHOLD = 0.95
PAIRS = [
    ("the cat sat on the mat", "the cat sat on the mat"),
    ("the cat sat on the mat", "quantum chromodynamics lecture"),
]


def toy_embed(text):
    """A cheap bag-of-letters vector, so cosine reflects real text overlap."""
    buckets = [0.0] * 8
    for ch in text.lower():
        if ch.isalpha():
            buckets[ord(ch) % 8] += 1.0

    return buckets


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    if not norm:
        return 0.0

    return dot / norm


class MLGateway:
    """Fake async request/reply ML backend: submit(kind, payload) awaits the reply.
    Counts in-flight requests to show that the two encodes overlap."""

    def __init__(self):
        self.active = 0
        self.peak = 0

    async def submit(self, kind, payload):
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(REPLY_DELAY)
        self.active -= 1

        return {"vector": toy_embed(payload["text"])}


class Fork(Task):
    """Splits the (query, doc) pair to the two encoders."""

    def execute(self, pair):
        yield Downstream(pair[0], output="query")
        yield Downstream(pair[1], output="doc")


class Encode(Task):
    """Submits one text for embedding; forwards the vector when the reply lands."""

    def __init__(self, gateway, **kwargs):
        super().__init__(**kwargs)
        self.gateway = gateway

    async def execute(self, text):
        reply = await self.gateway.submit("embed", {"text": text})

        return Downstream(reply["vector"])


class Compare(Task):
    """Merge node: runs once both vectors have arrived, emits their cosine similarity."""

    def execute(self, query_vec, doc_vec):
        return Downstream(cosine(query_vec, doc_vec))


class ShapeResponse(Task):
    """Turns the raw score into the client-facing reply and leaves the graph."""

    def execute(self, score):
        return GraphOutput({"match": score >= MATCH_THRESHOLD, "score": round(score, 4)})


def build(gateway):
    fork = Fork()()
    encode_query = Encode(gateway)()
    encode_doc = Encode(gateway)()
    compare = Compare()()
    shape = ShapeResponse()()

    fork["query"] >> encode_query
    fork["doc"] >> encode_doc
    encode_query >> compare["query_vec"]
    encode_doc >> compare["doc_vec"]
    compare >> shape

    return fork


async def main():
    gateway = MLGateway()
    print("two concurrent encodes -> compare -> shape, one gateway behind them")
    for query, doc in PAIRS:
        executor = Executor(max_parallel=8)
        async for reply in executor.run(build(gateway), (query, doc)):
            print(f"  query={query!r} doc={doc!r} -> {reply}")

    print("peak concurrent gateway requests:", gateway.peak)


if __name__ == "__main__":
    asyncio.run(main())
