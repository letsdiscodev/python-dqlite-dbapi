"""Re-executing a cursor resets fetch position and ``_rows`` to the new result set."""

from unittest.mock import MagicMock

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor
from dqlitewire.constants import ValueType

INTEGER = ValueType.INTEGER


class _AwaitableObj:
    def __init__(self, obj: object) -> None:
        self.obj = obj

    def __await__(self):
        yield from ()
        return self.obj


class _ScriptedClient:
    def __init__(self, scripted: list[tuple[list[str], list, list[list], list[list]]]) -> None:  # type: ignore[type-arg]
        self._scripted = scripted
        self._idx = 0

    def query_raw_typed(self, sql: str, params):
        result = self._scripted[self._idx]
        self._idx += 1
        return _AwaitableObj(obj=result)

    def execute(self, sql: str, params):
        return _AwaitableObj(obj=(0, 0))


async def test_sync_cursor_iterator_resets_on_reexecute() -> None:
    conn = MagicMock()
    scripted = _ScriptedClient(
        [
            (["x"], [INTEGER], [[], [], []], [[1], [2], [3]]),
            (["x"], [INTEGER], [[], []], [[4], [5]]),
        ]
    )

    async def get_client():
        return scripted

    conn._get_async_connection = get_client

    c = Cursor(conn)

    await c._execute_async("SELECT x FROM t")
    it = iter(c)
    assert next(it) == (1,)

    await c._execute_async("SELECT x FROM u")
    rows = list(c)
    assert rows == [(4,), (5,)]


async def test_async_cursor_iterator_resets_on_reexecute() -> None:
    """Async parity of the sync test."""
    import asyncio

    conn = MagicMock()
    conn._closed = False
    lock = asyncio.Lock()

    scripted = _ScriptedClient(
        [
            (["x"], [INTEGER], [[], [], []], [[1], [2], [3]]),
            (["x"], [INTEGER], [[], []], [[4], [5]]),
        ]
    )

    async def fake_ensure_connection():
        return scripted

    conn._ensure_connection = fake_ensure_connection
    conn._ensure_locks = MagicMock(return_value=(lock, lock))

    c = AsyncCursor(conn)

    await c.execute("SELECT x FROM t")
    first = await c.__anext__()
    assert first == (1,)

    await c.execute("SELECT x FROM u")
    rows: list[tuple] = []  # type: ignore[type-arg]
    async for row in c:
        rows.append(row)
    assert rows == [(4,), (5,)]
