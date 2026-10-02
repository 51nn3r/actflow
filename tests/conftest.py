import asyncio

import pytest
from actflow import AsyncExecutor


@pytest.fixture
def walk():
    """Walk a graph to the end and return everything it handed out."""

    def _walk(start, seed=None, **kwargs):
        async def go():
            return [item async for item in AsyncExecutor(**kwargs).run(start, seed)]

        return asyncio.run(go())

    return _walk