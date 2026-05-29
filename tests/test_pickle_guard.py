"""Connection/Cursor/AsyncConnection/AsyncCursor refuse to pickle with a clear TypeError.

Mirrors stdlib sqlite3's explicit driver-level error instead of the default pickle walk's
confusing "cannot pickle '_thread.lock'".
"""

from __future__ import annotations

import pickle

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


class TestPickleGuard:
    def test_connection_refuses_to_pickle_with_clear_error(self) -> None:
        conn = Connection.__new__(Connection)
        with pytest.raises(TypeError, match=r"cannot pickle 'Connection'"):
            pickle.dumps(conn)

    def test_cursor_refuses_to_pickle_with_clear_error(self) -> None:
        cur = Cursor.__new__(Cursor)
        with pytest.raises(TypeError, match=r"cannot pickle 'Cursor'"):
            pickle.dumps(cur)

    def test_async_connection_refuses_to_pickle_with_clear_error(self) -> None:
        conn = AsyncConnection.__new__(AsyncConnection)
        with pytest.raises(TypeError, match=r"cannot pickle 'AsyncConnection'"):
            pickle.dumps(conn)

    def test_async_cursor_refuses_to_pickle_with_clear_error(self) -> None:
        cur = AsyncCursor.__new__(AsyncCursor)
        with pytest.raises(TypeError, match=r"cannot pickle 'AsyncCursor'"):
            pickle.dumps(cur)

    def test_connection_error_message_names_actionable_alternative(self) -> None:
        conn = Connection.__new__(Connection)
        with pytest.raises(TypeError) as excinfo:
            pickle.dumps(conn)
        assert "consumer process" in str(excinfo.value)


class TestCopyGuard:
    """copy.copy/deepcopy route through __reduce__ (pickle's path), so they reject too."""

    @pytest.mark.parametrize("cls", [Connection, Cursor, AsyncConnection, AsyncCursor])
    def test_copy_copy_refuses_with_clear_error(self, cls: type) -> None:
        import copy

        instance = cls.__new__(cls)  # type: ignore[call-overload]
        with pytest.raises(TypeError, match=f"cannot pickle '{cls.__name__}'"):
            copy.copy(instance)

    @pytest.mark.parametrize("cls", [Connection, Cursor, AsyncConnection, AsyncCursor])
    def test_copy_deepcopy_refuses_with_clear_error(self, cls: type) -> None:
        import copy

        instance = cls.__new__(cls)  # type: ignore[call-overload]
        with pytest.raises(TypeError, match=f"cannot pickle '{cls.__name__}'"):
            copy.deepcopy(instance)
