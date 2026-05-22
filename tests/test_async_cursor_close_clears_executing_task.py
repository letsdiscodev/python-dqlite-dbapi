"""Pin: ``AsyncCursor.close()`` clears ``_executing_task`` alongside
the rest of the per-execute state.

The execute / executemany entry points re-check ``_check_closed``
first, so a stale ``_executing_task`` on a closed cursor is not
load-bearing on the operational path. The scrub keeps the closed-
cursor invariant uniform: any introspection path (debug helpers,
third-party event hooks) that reads ``_executing_task`` on a closed
cursor sees ``None`` rather than the completed task that ran the
final ``execute()``.

Sibling scrubs already pinned in ``close()``:

- ``_rows`` / ``_description`` / ``_rowcount`` / ``_lastrowid`` /
  ``_row_index`` (see ``test_aio_cursor_close_scrubs_state.py``)
- ``_connection`` → ``weakref.proxy`` swap

``_executing_task`` was the residual slot.
"""

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
    # Simulate state at the end of a successful execute: the
    # ``_executing_task`` was set during execute entry and would
    # normally be cleared on the success path. To pin the close-time
    # scrub, install a non-None task and call close() directly.
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
    # Second close (PEP 249 idempotent contract) keeps the scrub.
    cur.close()
    assert cur._executing_task is None
