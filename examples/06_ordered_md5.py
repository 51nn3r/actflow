"""actflow example 06: ordered input and ordered output around one md5 node.

Messages arrive shuffled: the ordered input holds each until its idx comes up.
Ticks overlap and the earliest is slowest, so digests finish out of order;
the paired ordered output releases them by ascending idx anyway.
"""

import asyncio
import hashlib

from actflow import (
    Downstream,
    Executor,
    GraphOutput,
    OrderedInputController,
    OrderedOutputController,
    Task,
)

ARRIVALS = [
    {"idx": 0, "user": "user1", "text": "first message in the thread"},
    {"idx": 2, "user": "user3", "text": "third message in the thread"},
    {"idx": 1, "user": "user2", "text": "second message in the thread"},
]


class Feed(Task):
    """Puts each message on the wire in arrival order; every message carries its idx."""

    def execute(self, arrivals):
        for message in arrivals:
            print(f"arrived: {message['user']} (idx={message['idx']})")
            yield Downstream(message, output="msg")


class Md5(Task):
    """Hashes one message per tick; the earliest tick sleeps longest, so ticks overlap."""

    async def execute(self, msg):
        await asyncio.sleep((3 - msg["idx"]) * 0.03)
        digest = hashlib.md5(msg["text"].encode()).hexdigest()
        print(f"  [md5] idx={msg['idx']} ({msg['user']}) -> {digest[:8]}")

        return GraphOutput((msg["idx"], digest))


def build():
    feed = Feed()()
    md5 = Md5(
        input_controller=OrderedInputController(),
        output_controller=OrderedOutputController(),
    )()
    feed["msg"] >> md5

    return feed


async def main():
    print("message log: the tail of the thread arrives swapped")
    feed = build()
    executor = Executor(max_parallel=4)
    results = [out async for out in executor.run(feed, ARRIVALS)]

    print("digests (by ascending idx):", results)
    print("order restored:", results == sorted(results))


if __name__ == "__main__":
    asyncio.run(main())
