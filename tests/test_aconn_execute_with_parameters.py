"""AsyncConnection.execute(sql, params) forwards the parameters to the cursor.

Existing async pins exercise only the no-params arm.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


async def test_aconn_execute_with_parameters_forwards_to_cursor() -> None:
    """execute(sql, params) dispatches both the operation and the parameters to the cursor."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    cursors_seen: list[AsyncCursor] = []
    seen_calls: list[tuple[object, ...]] = []

    def fake_cursor() -> AsyncCursor:
        cur = MagicMock(spec=AsyncCursor)

        async def _execute(*args: object) -> None:
            seen_calls.append(args)

        cur.execute = AsyncMock(side_effect=_execute)
        cur.close = AsyncMock()
        cursors_seen.append(cur)
        return cur

    aconn.cursor = fake_cursor  # type: ignore[assignment]
    aconn.messages = []

    cur = await aconn.execute("SELECT ?", [1])

    assert cur is cursors_seen[0]
    assert seen_calls == [("SELECT ?", [1])]
    close_mock = cursors_seen[0].close
    assert isinstance(close_mock, AsyncMock)
    close_mock.assert_not_called()
