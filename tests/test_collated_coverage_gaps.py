"""Coverage-gap tests collated from a focused audit of reachable but
untested branches in dqlitedbapi.

Each test is deliberately narrow — pinning a single branch's
behaviour so a future refactor cannot silently flip it.
"""

from __future__ import annotations

import asyncio

import pytest

import dqlitedbapi
import dqlitedbapi.aio as dqlite_aio
from dqlitedbapi.exceptions import NotSupportedError


class TestAioConnectUnknownKwargsRejection:
    """Async ``aconnect()`` mirrors the sync sibling's rejection of
    stdlib-sqlite3 kwargs (``database``, ``timeout`` aliases, etc.).
    The sync side is parametrised; the async side was uncovered."""

    def test_rejects_stdlib_sqlite3_kwargs(self) -> None:
        async def _drive() -> None:
            with pytest.raises(NotSupportedError):
                # connect's signature uses **unknown_kwargs to absorb
                # any unrecognised kwarg and surface NotSupportedError;
                # passing a stdlib sqlite3 kwarg drives the rejection
                # arm. ``isolation_level`` is one such kwarg.
                # The await is on the synchronous-rejection path:
                # connect raises before any coroutine work, so the
                # pytest.raises wraps a sync raise, not an awaitable.
                _ = dqlite_aio.connect(
                    "127.0.0.1:9001",
                    isolation_level=None,
                )

        asyncio.run(_drive())


class TestConnectionRowFactoryHook:
    """``Connection.row_factory`` is a Python-side hook (stdlib parity).
    Default is None; setter accepts callable or None and rejects
    non-callable with ``ProgrammingError``. New cursors inherit the
    Connection's default."""

    def test_sync_connection_row_factory_default_is_none(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        conn._row_factory = None  # __new__ skips __init__
        assert conn.row_factory is None

    def test_sync_connection_row_factory_set_none_is_noop(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        conn._row_factory = None
        conn.row_factory = None  # no error
        assert conn.row_factory is None

    def test_sync_connection_row_factory_accepts_callable(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        conn._row_factory = None
        factory = lambda cur, row: row  # noqa: E731
        conn.row_factory = factory
        assert conn.row_factory is factory

    def test_sync_connection_row_factory_rejects_non_callable(self) -> None:
        from dqlitedbapi.connection import Connection
        from dqlitedbapi.exceptions import ProgrammingError

        conn = Connection.__new__(Connection)
        conn._row_factory = None
        with pytest.raises(ProgrammingError):
            conn.row_factory = 42


class TestConnectionTextFactoryRejection:
    """``Connection.text_factory`` returns ``str`` (always) on read;
    the setter accepts ``str`` (no-op) and rejects everything else with
    ``NotSupportedError``."""

    def test_sync_connection_text_factory_get_returns_str(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        assert conn.text_factory is str

    def test_sync_connection_text_factory_set_str_is_noop(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        conn.text_factory = str  # no error

    def test_sync_connection_text_factory_set_non_str_rejected(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        with pytest.raises(NotSupportedError):
            conn.text_factory = bytes


class TestSyncCursorExecutescriptStub:
    """The sync ``Cursor.executescript`` stub raises NotSupportedError;
    the async sibling is pinned by an existing test, the sync one was
    uncovered."""

    def test_sync_cursor_executescript_raises_not_supported(self) -> None:
        from unittest.mock import MagicMock

        from dqlitedbapi.cursor import Cursor

        conn = MagicMock()
        cur = Cursor(conn)
        with pytest.raises(NotSupportedError):
            cur.executescript("SELECT 1")


class TestExceptionReprBranches:
    """``OperationalError`` / ``IntegrityError`` / ``InternalError`` repr
    branches with and without ``code`` are covered. ``InterfaceError``
    repr was uncovered for the no-code branch."""

    def test_interface_error_repr_without_code(self) -> None:
        e = dqlitedbapi.InterfaceError("plain message")
        # The repr should embed the message; the precise format is the
        # important pin.
        rendered = repr(e)
        assert "plain message" in rendered
