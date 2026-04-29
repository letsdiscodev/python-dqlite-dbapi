"""Pin diagnostic-quality fields in user-facing error messages and
reprs:

- The async loop-affinity ``ProgrammingError`` includes both the
  bound and current loop identifiers (mirrors the sync sibling's
  thread-affinity message which already names both thread ids).
- ``Cursor.__repr__`` / ``AsyncCursor.__repr__`` include the parent
  connection's address and ``id(self)`` so logs that fan multiple
  cursors across pooled connections can be disambiguated.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import (
    AsyncConnection,
    _format_loop_affinity_message,
)
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


class TestLoopAffinityMessageNamesBothLoops:
    def test_message_includes_both_loop_ids(self) -> None:
        bound = asyncio.new_event_loop()
        current = asyncio.new_event_loop()
        try:
            msg = _format_loop_affinity_message(bound, current, "was first used")
            assert f"0x{id(bound):x}" in msg
            assert f"0x{id(current):x}" in msg
            assert "was first used" in msg
        finally:
            bound.close()
            current.close()

    def test_message_marks_garbage_collected_loop(self) -> None:
        current = asyncio.new_event_loop()
        try:
            msg = _format_loop_affinity_message(None, current, ".cursor()")
            assert "garbage-collected" in msg
            assert f"0x{id(current):x}" in msg
        finally:
            current.close()

    def test_message_marks_closed_loop(self) -> None:
        bound = asyncio.new_event_loop()
        current = asyncio.new_event_loop()
        try:
            bound.close()
            msg = _format_loop_affinity_message(bound, current, "was first used")
            assert f"0x{id(bound):x}" in msg
            assert "(closed)" in msg
        finally:
            current.close()

    def test_message_handles_no_running_loop(self) -> None:
        bound = asyncio.new_event_loop()
        try:
            msg = _format_loop_affinity_message(bound, None, ".cursor()")
            assert "no running loop" in msg
        finally:
            bound.close()


class TestCursorReprIncludesAddressAndId:
    def test_sync_cursor_repr_includes_address_and_id(self) -> None:
        cur = Cursor.__new__(Cursor)
        cur._closed = False
        cur._rowcount = -1
        fake_conn = MagicMock()
        fake_conn._address = "10.0.0.1:9001"
        cur._connection = fake_conn
        r = repr(cur)
        assert "Cursor" in r
        assert "10.0.0.1:9001" in r
        assert f"0x{id(cur):x}" in r
        assert "open" in r

    def test_sync_cursor_repr_falls_back_when_address_missing(self) -> None:
        cur = Cursor.__new__(Cursor)
        cur._closed = True
        cur._rowcount = 5
        cur._connection = MagicMock(spec=[])  # no _address
        r = repr(cur)
        assert "address='?'" in r
        assert "closed" in r
        assert "rowcount=5" in r

    def test_async_cursor_repr_includes_address_and_id(self) -> None:
        cur = AsyncCursor.__new__(AsyncCursor)
        cur._closed = False
        cur._rowcount = -1
        fake_conn = MagicMock()
        fake_conn._address = "10.0.0.2:9001"
        cur._connection = fake_conn
        r = repr(cur)
        assert "AsyncCursor" in r
        assert "10.0.0.2:9001" in r
        assert f"0x{id(cur):x}" in r

    def test_async_cursor_repr_falls_back_when_address_missing(self) -> None:
        cur = AsyncCursor.__new__(AsyncCursor)
        cur._closed = True
        cur._rowcount = 0
        cur._connection = MagicMock(spec=[])
        r = repr(cur)
        assert "address='?'" in r


class TestLoopAffinityMessageAtCallSites:
    @pytest.mark.asyncio
    async def test_ensure_locks_raises_with_loop_ids(self) -> None:
        """Drive the actual call site: bind on loop A, attempt use on
        loop B, assert message contains the loop ids."""
        import os as _os

        conn = AsyncConnection.__new__(AsyncConnection)
        conn._closed = False
        conn._creator_pid = _os.getpid()
        # Bind to a fake loop reference.
        import weakref

        bound = asyncio.new_event_loop()
        try:
            conn._loop_ref = weakref.ref(bound)
            conn._connect_lock = MagicMock()
            conn._op_lock = MagicMock()
            current = asyncio.get_running_loop()
            from dqlitedbapi.exceptions import ProgrammingError

            with pytest.raises(ProgrammingError) as excinfo:
                conn._ensure_locks()
            msg = str(excinfo.value)
            assert f"0x{id(bound):x}" in msg
            assert f"0x{id(current):x}" in msg
        finally:
            bound.close()
