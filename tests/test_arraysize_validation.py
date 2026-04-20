"""Cursor.arraysize rejects non-positive values."""

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


class TestArraysizeValidation:
    def _sync_cursor(self) -> Cursor:
        conn = Connection("localhost:19001", timeout=2.0)
        return Cursor(conn)

    def _async_cursor(self) -> AsyncCursor:
        conn = AsyncConnection("localhost:19001")
        return AsyncCursor(conn)

    def test_zero_rejected_sync(self) -> None:
        c = self._sync_cursor()
        with pytest.raises(ProgrammingError, match=">= 1"):
            c.arraysize = 0

    def test_negative_rejected_sync(self) -> None:
        c = self._sync_cursor()
        with pytest.raises(ProgrammingError, match=">= 1"):
            c.arraysize = -5

    def test_positive_accepted_sync(self) -> None:
        c = self._sync_cursor()
        c.arraysize = 10
        assert c.arraysize == 10

    def test_zero_rejected_async(self) -> None:
        c = self._async_cursor()
        with pytest.raises(ProgrammingError, match=">= 1"):
            c.arraysize = 0

    def test_negative_rejected_async(self) -> None:
        c = self._async_cursor()
        with pytest.raises(ProgrammingError, match=">= 1"):
            c.arraysize = -1

    # Non-int types: PEP 249 says arraysize is an int attribute. A
    # ``None`` / ``str`` / ``float`` / ``bool`` assignment must surface
    # a clean ProgrammingError at assignment time, not a downstream
    # TypeError from a comparison or a silently-stored non-int.

    def test_none_rejected_sync(self) -> None:
        c = self._sync_cursor()
        with pytest.raises(ProgrammingError, match="NoneType"):
            c.arraysize = None  # type: ignore[assignment]

    def test_str_rejected_sync(self) -> None:
        c = self._sync_cursor()
        with pytest.raises(ProgrammingError, match="str"):
            c.arraysize = "5"  # type: ignore[assignment]

    def test_float_rejected_sync(self) -> None:
        c = self._sync_cursor()
        with pytest.raises(ProgrammingError, match="float"):
            c.arraysize = 1.5  # type: ignore[assignment]

    def test_bool_rejected_sync(self) -> None:
        c = self._sync_cursor()
        with pytest.raises(ProgrammingError, match="bool"):
            c.arraysize = True  # type: ignore[assignment]

    def test_none_rejected_async(self) -> None:
        c = self._async_cursor()
        with pytest.raises(ProgrammingError, match="NoneType"):
            c.arraysize = None  # type: ignore[assignment]

    def test_str_rejected_async(self) -> None:
        c = self._async_cursor()
        with pytest.raises(ProgrammingError, match="str"):
            c.arraysize = "5"  # type: ignore[assignment]

    def test_float_rejected_async(self) -> None:
        c = self._async_cursor()
        with pytest.raises(ProgrammingError, match="float"):
            c.arraysize = 1.5  # type: ignore[assignment]

    def test_bool_rejected_async(self) -> None:
        c = self._async_cursor()
        with pytest.raises(ProgrammingError, match="bool"):
            c.arraysize = True  # type: ignore[assignment]
