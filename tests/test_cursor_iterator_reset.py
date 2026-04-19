"""Cursor iterator state resets when execute() is called again.

PEP 249 allows re-executing a cursor. The fetch position and
``_rows`` buffer must move to the new result set so ``next(iter(
cursor))`` and ``list(cursor)`` both start from row 0 of the new
SELECT — not from wherever the previous iteration left off.
"""

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor


class _AwaitableObj:
    def __init__(self, obj: object) -> None:
        self.obj = obj

    def __await__(self):  # type: ignore[no-untyped-def]
        yield from ()
        return self.obj


class _ScriptedClient:
    """Replays pre-canned ``query_raw_typed`` responses in order."""

    def __init__(self, scripted: list[tuple[list[str], list, list[list]]]) -> None:
        self._scripted = scripted
        self._idx = 0

    def query_raw_typed(self, sql: str, params):  # type: ignore[no-untyped-def]
        result = self._scripted[self._idx]
        self._idx += 1
        return _AwaitableObj(obj=result)

    def execute(self, sql: str, params):  # type: ignore[no-untyped-def]
        return _AwaitableObj(obj=(0, 0))


@pytest.mark.asyncio
async def test_sync_cursor_iterator_resets_on_reexecute() -> None:
    """After a second ``execute``, iterating must yield the new
    result set in full — not continue from the prior index."""
    conn = MagicMock()
    scripted = _ScriptedClient(
        [
            (["x"], [], [[1], [2], [3]]),
            (["x"], [], [[4], [5]]),
        ]
    )

    async def get_client():  # type: ignore[no-untyped-def]
        return scripted

    conn._get_async_connection = get_client

    c = Cursor(conn)

    await c._execute_async("SELECT x FROM t")
    it = iter(c)
    assert next(it) == (1,)  # consume first row; _row_index is now 1

    await c._execute_async("SELECT x FROM u")
    rows = list(c)
    assert rows == [(4,), (5,)]
