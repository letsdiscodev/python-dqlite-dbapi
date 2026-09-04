"""``AsyncCursor.close()`` clears ``_executing_task`` so closed-cursor introspection sees ``None``
rather than the completed task that ran the final ``execute()``."""

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor

pytestmark = pytest.mark.asyncio


def _bare_async_cursor() -> AsyncCursor:
    aconn = MagicMock()
    aconn._closed = False
    acur = AsyncCursor.__new__(AsyncCursor)
    acur._closed = False
    acur._connection = aconn
    acur._executing_task = None
    acur.messages = []
    acur._rows = []
    acur._description = None
    acur._rowcount = -1
    acur._lastrowid = None
    acur._row_index = 0
    return acur


async def test_close_clears_executing_task_when_set() -> None:
    cur = _bare_async_cursor()
    # Install a non-None task to pin the close-time scrub.
    cur._executing_task = asyncio.current_task()
    assert cur._executing_task is not None

    cur.close()

    assert cur._executing_task is None, (
        "AsyncCursor.close() must scrub _executing_task alongside the "
        "rest of the per-execute state so closed-cursor introspection "
        "sees a uniform 'no operation performed' surface; got "
        f"{cur._executing_task!r}"
    )


async def test_close_keeps_executing_task_none_when_already_none() -> None:
    cur = _bare_async_cursor()
    assert cur._executing_task is None
    cur.close()
    assert cur._executing_task is None


async def test_close_clear_is_idempotent() -> None:
    cur = _bare_async_cursor()
    cur._executing_task = asyncio.current_task()
    cur.close()
    assert cur._executing_task is None
    cur.close()  # idempotent (PEP 249)
    assert cur._executing_task is None
