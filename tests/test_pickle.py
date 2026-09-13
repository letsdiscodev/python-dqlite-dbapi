"""Exception pickling (raw message, cause) and the connection/cursor pickle guard."""

from __future__ import annotations

import copy
import pickle

import pytest

import dqlitedbapi
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)


@pytest.mark.parametrize(
    "cls",
    [
        OperationalError,
        IntegrityError,
        DatabaseError,
        DataError,
        InternalError,
        ProgrammingError,
    ],
)
def test_error_pickle_preserves_raw_message_and_code(cls: type) -> None:
    e = cls("truncated msg", 42, raw_message="full server text " * 100)
    blob = pickle.dumps(e)
    restored = pickle.loads(blob)
    assert restored.raw_message == e.raw_message
    assert restored.code == 42
    assert isinstance(restored, cls)


def test_not_supported_error_pickle_round_trip() -> None:
    """``NotSupportedError`` takes a single message arg (no code, no raw_message)."""
    e = NotSupportedError("dqlite has no foo")
    restored = pickle.loads(pickle.dumps(e))
    assert isinstance(restored, NotSupportedError)
    assert str(restored) == str(e)


def test_warning_pickle_round_trip() -> None:
    """``Warning`` is the PEP 249 sibling of ``Error`` (NOT a subclass)."""
    e = dqlitedbapi.Warning("truncation notice")
    restored = pickle.loads(pickle.dumps(e))
    assert isinstance(restored, dqlitedbapi.Warning)
    assert str(restored) == str(e)


def test_interface_error_pickle_preserves_raw_message_and_code() -> None:
    e = InterfaceError("wire problem", 1001, raw_message="DQLITE_PROTO ...")
    blob = pickle.dumps(e)
    restored = pickle.loads(blob)
    assert restored.raw_message == "DQLITE_PROTO ..."
    assert restored.code == 1001


def test_error_deepcopy_preserves_raw_message_and_code() -> None:
    """deepcopy uses the same ``__reduce__`` path."""
    e = OperationalError("msg", 10, raw_message="raw text")
    e2 = copy.deepcopy(e)
    assert e2.raw_message == "raw text"
    assert e2.code == 10


def test_database_error_cause_pickle_either_or() -> None:
    inner = ValueError("inner forensic state")
    outer = DatabaseError("outer wrap")
    outer.__cause__ = inner
    restored = pickle.loads(pickle.dumps(outer))
    if restored.__cause__ is not None:
        assert isinstance(restored.__cause__, ValueError)


def test_interface_error_cause_deepcopy_either_or() -> None:
    """deepcopy goes through __reduce__ same as pickle."""
    inner = RuntimeError("inner")
    outer = InterfaceError("outer")
    outer.__cause__ = inner
    restored = copy.deepcopy(outer)
    if restored.__cause__ is not None:
        assert isinstance(restored.__cause__, RuntimeError)


def test_operational_error_cause_pickle_either_or() -> None:
    inner = ValueError("wire-level cause")
    outer = OperationalError("dbapi msg", 1)
    outer.__cause__ = inner
    restored = pickle.loads(pickle.dumps(outer))
    if restored.__cause__ is not None:
        assert isinstance(restored.__cause__, ValueError)
    assert "dbapi msg" in str(restored)


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
