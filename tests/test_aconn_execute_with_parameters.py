"""Pin: ``AsyncConnection.execute(sql, params)`` parameter-forwarding.

The shortcut in ``AsyncConnection.execute`` (the
``await cur.execute(operation, parameters)`` arm) is the parametrised
forwarding path; the no-params arm of the same shortcut is exercised
by
``test_audit_2026_05_dbapi_coverage.py::test_async_execute_shortcut_closes_cursor_on_raise``.

Without this pin, a refactor that mishandles the parameter forwarding
(e.g. ``await cur.execute(operation)`` accidentally on both branches)
would pass the existing test suite because every existing async pin
exercises only the no-params arm.

Sibling to the sync coverage carried by the regular
``Connection.execute`` test suite — completes the parametrised-arm
pin set across both surfaces.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


async def test_aconn_execute_with_parameters_forwards_to_cursor() -> None:
    """``AsyncConnection.execute(sql, params)`` dispatches BOTH the
    operation AND the parameters to the underlying cursor's
    ``execute`` call. A refactor that drops ``parameters`` on the
    forwarding path would surface here."""
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

    # The same cursor object the shortcut opened is returned.
    assert cur is cursors_seen[0]
    # The forwarding call passed BOTH the SQL AND the parameters.
    assert seen_calls == [("SELECT ?", [1])]
    # Successful happy-path: cursor.close was NOT called.
    close_mock = cursors_seen[0].close
    assert isinstance(close_mock, AsyncMock)
    close_mock.assert_not_called()
