"""Coverage-gap tests, each pinning a single reachable-but-untested branch in dqlitedbapi."""

from __future__ import annotations

import asyncio
import os
import threading

import pytest

import dqlitedbapi
import dqlitedbapi.aio as dqlite_aio
from dqlitedbapi.exceptions import NotSupportedError


class TestAioConnectLazyUnknownKwargsRejection:
    """dqlite_aio.connect() is the lazy helper; its **unknown_kwargs rejection runs before
    the constructor and must be covered so a kwargs-set refactor can't relax it."""

    def test_rejects_stdlib_sqlite3_kwargs(self) -> None:
        async def _drive() -> None:
            with pytest.raises(NotSupportedError):
                # A stdlib sqlite3 kwarg drives the **unknown_kwargs rejection arm.
                # connect raises synchronously before any coroutine work.
                _ = dqlite_aio.connect(
                    "127.0.0.1:9001",
                    detect_types=1,
                )

        asyncio.run(_drive())


class TestAioAConnectUnknownKwargsRejection:
    """dqlite_aio.aconnect() is the eager helper; its **unknown_kwargs rejection is a
    distinct code path from connect() and fires before any TCP work."""

    @pytest.mark.asyncio
    async def test_rejects_stdlib_sqlite3_kwargs(self) -> None:
        with pytest.raises(NotSupportedError):
            await dqlite_aio.aconnect(
                "127.0.0.1:9001",
                detect_types=1,
            )

    @pytest.mark.asyncio
    async def test_rejects_unknown_kwarg(self) -> None:
        with pytest.raises(NotSupportedError):
            await dqlite_aio.aconnect(
                "127.0.0.1:9001",
                this_kwarg_does_not_exist=42,
            )


class TestConnectionRowFactoryHook:
    """Connection.row_factory: default None, setter accepts callable/None, rejects others."""

    def test_sync_connection_row_factory_default_is_none(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        # Setter enforces thread-affinity; seed the sentinels so the same-thread call passes.
        conn._row_factory = None
        conn._creator_pid = os.getpid()
        conn._creator_thread = threading.get_ident()  # __new__ skips __init__
        assert conn.row_factory is None

    def test_sync_connection_row_factory_set_none_is_noop(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        # Setter enforces thread-affinity; seed the sentinels so the same-thread call passes.
        conn._row_factory = None
        conn._creator_pid = os.getpid()
        conn._creator_thread = threading.get_ident()
        conn.row_factory = None
        assert conn.row_factory is None

    def test_sync_connection_row_factory_accepts_callable(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        # Setter enforces thread-affinity; seed the sentinels so the same-thread call passes.
        conn._row_factory = None
        conn._creator_pid = os.getpid()
        conn._creator_thread = threading.get_ident()
        factory = lambda cur, row: row  # noqa: E731
        conn.row_factory = factory
        assert conn.row_factory is factory

    def test_sync_connection_row_factory_rejects_non_callable(self) -> None:
        from dqlitedbapi.connection import Connection
        from dqlitedbapi.exceptions import ProgrammingError

        conn = Connection.__new__(Connection)
        # Setter enforces thread-affinity; seed the sentinels so the same-thread call passes.
        conn._row_factory = None
        conn._creator_pid = os.getpid()
        conn._creator_thread = threading.get_ident()
        with pytest.raises(ProgrammingError):
            conn.row_factory = 42


class TestConnectionTextFactoryRejection:
    """Connection.text_factory reads as str; setter accepts str, rejects others."""

    def test_sync_connection_text_factory_get_returns_str(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        assert conn.text_factory is str

    def test_sync_connection_text_factory_set_str_is_noop(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        # Setter calls _check_thread(); seed affinity fields to reach the value-validation arm.
        conn._creator_pid = os.getpid()
        conn._creator_thread = threading.get_ident()
        conn.text_factory = str

    def test_sync_connection_text_factory_set_non_str_rejected(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
        conn._creator_pid = os.getpid()
        conn._creator_thread = threading.get_ident()
        with pytest.raises(NotSupportedError):
            conn.text_factory = bytes


class TestSyncCursorExecutescriptStub:
    """The sync Cursor.executescript stub raises NotSupportedError."""

    def test_sync_cursor_executescript_raises_not_supported(self) -> None:
        from unittest.mock import MagicMock

        from dqlitedbapi.cursor import Cursor

        conn = MagicMock()
        cur = Cursor(conn)
        with pytest.raises(NotSupportedError):
            cur.executescript("SELECT 1")


class TestExceptionReprBranches:
    """Cover InterfaceError's no-code repr branch."""

    def test_interface_error_repr_without_code(self) -> None:
        e = dqlitedbapi.InterfaceError("plain message")
        rendered = repr(e)
        assert "plain message" in rendered
