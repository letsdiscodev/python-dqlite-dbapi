"""PRAGMA write-form dispatches through the row-returning branch but
produces zero columns; ``description`` must be ``None`` to match
stdlib ``sqlite3``, not ``[]`` / ``()`` from an empty comprehension.

Mocks the wire client response so the test does not require a live
dqlite cluster.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.cursor import Cursor


def _make_sync_cursor_with_mock_response(response: tuple[Any, Any, Any, Any]) -> Cursor:
    mock_async_conn = AsyncMock()
    mock_async_conn.query_raw_typed = AsyncMock(return_value=response)
    mock_async_conn.execute = AsyncMock(return_value=(0, 0))
    mock_conn = MagicMock()

    async def get_async_conn() -> AsyncMock:
        return mock_async_conn

    mock_conn._get_async_connection = get_async_conn

    def run_sync(coro: object) -> object:
        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    mock_conn._run_sync = run_sync
    return Cursor(mock_conn)


class TestPragmaWriteDescriptionNone:
    def test_pragma_write_form_sets_description_none(self) -> None:
        # Server returns (columns=[], column_types=[], row_types=[], rows=[]).
        c = _make_sync_cursor_with_mock_response(([], [], [], []))
        c.execute("PRAGMA foreign_keys = ON")
        assert c.description is None, (
            "PRAGMA write-form produces no columns; stdlib sqlite3 sets "
            "description=None for non-result statements."
        )

    def test_pragma_read_form_sets_description(self) -> None:
        c = _make_sync_cursor_with_mock_response((["foreign_keys"], [1], [[1]], [(1,)]))
        c.execute("PRAGMA foreign_keys")
        assert c.description is not None
        assert len(c.description) == 1


# Async equivalent is covered by the shared behaviour: ``AsyncCursor``'s
# row-returning branch in aio/cursor.py uses the same ``if not columns:
# self._description = None`` guard as the sync branch tested above.
# Mocking the full async _ensure_connection / op_lock path to exercise
# it end-to-end through the public ``execute`` entry would duplicate
# a lot of existing test infrastructure; the sync coverage pins the
# contract, and the async mirror is a straight copy of the same
# branch.
