"""Exception hierarchy, repr/code, message caps, sqlite_errorname, and stub error normalization."""

from __future__ import annotations

import pickle
import sqlite3

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
    Warning,
)


class TestExceptionRaising:
    def test_raise_warning(self) -> None:
        with pytest.raises(Warning):
            raise Warning("test warning")

    def test_raise_error(self) -> None:
        with pytest.raises(Error):
            raise Error("test error")

    def test_raise_interface_error(self) -> None:
        with pytest.raises(InterfaceError):
            raise InterfaceError("interface error")

    def test_raise_database_error(self) -> None:
        with pytest.raises(DatabaseError):
            raise DatabaseError("database error")

    def test_error_is_exception_subclass(self) -> None:
        assert issubclass(Error, Exception)

    def test_catch_database_error_as_error(self) -> None:
        with pytest.raises(Error):
            raise DatabaseError("test")

    def test_catch_operational_error_as_database_error(self) -> None:
        with pytest.raises(DatabaseError):
            raise OperationalError("test")


def test_operational_error_repr_includes_code() -> None:
    exc = OperationalError("busy", code=5)
    assert repr(exc) == "OperationalError('busy', code=5)"


def test_operational_error_repr_without_code() -> None:
    exc = OperationalError("plain")
    assert repr(exc) == "OperationalError('plain')"


def test_integrity_error_repr_includes_code() -> None:
    exc = IntegrityError("UNIQUE", code=2067)
    assert repr(exc) == "IntegrityError('UNIQUE', code=2067)"


def test_integrity_error_repr_without_code() -> None:
    exc = IntegrityError("constraint")
    assert repr(exc) == "IntegrityError('constraint')"


def test_internal_error_repr_includes_code() -> None:
    exc = InternalError("sqlite internal", code=2)
    assert repr(exc) == "InternalError('sqlite internal', code=2)"


def test_internal_error_repr_without_code() -> None:
    exc = InternalError("plain")
    assert repr(exc) == "InternalError('plain')"


def test_str_unchanged() -> None:
    """str(exc) still returns only the message, not the code."""
    assert str(OperationalError("plain", code=5)) == "plain"
    assert str(IntegrityError("x", code=2067)) == "x"
    assert str(InternalError("y", code=2)) == "y"


# 4 KiB cap + ~50-byte truncation suffix, plus headroom.
_CAP_BUDGET = 5000


@pytest.mark.parametrize(
    "cls",
    [
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
    ],
)
def test_message_arg_capped_at_4kb(cls: type) -> None:
    """A 63 KiB ``message`` produces a ``str(exc)`` bounded at the cap."""
    big = "X" * 63_000
    e = cls(big, code=42)
    rendered = str(e)
    assert len(rendered) < _CAP_BUDGET, (
        f"{cls.__name__} args[0] not capped: len(str(exc)) == {len(rendered)}"
    )


@pytest.mark.parametrize(
    "cls",
    [
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
    ],
)
def test_repr_capped_via_args0(cls: type) -> None:
    """``repr(exc)`` inherits the args[0] bound; 6 KiB absorbs escape inflation."""
    big = "X" * 63_000
    e = cls(big, code=42)
    rendered = repr(e)
    assert len(rendered) < 6000, (
        f"{cls.__name__} repr() not bounded: len(repr(exc)) == {len(rendered)}"
    )


def test_pickled_exception_bounded() -> None:
    """The pickled payload stays inside the budget once args[0] is capped."""
    big = "X" * 63_000
    e = OperationalError(big, code=1)
    pickled = pickle.dumps(e)
    assert len(pickled) < 16_000, (
        f"pickled OperationalError too large: {len(pickled)} bytes "
        f"(expected < 16 KiB after args[0] cap)"
    )


@pytest.mark.parametrize(
    "cls",
    [InterfaceError, DatabaseError, OperationalError],
)
def test_short_message_arg_round_trips(cls: type) -> None:
    """Short messages round-trip unchanged; the cap only fires past 4 KiB."""
    short = "ordinary error"
    e = cls(short, code=1)
    assert str(e) == short


@pytest.mark.parametrize(
    "cls",
    [
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
    ],
)
def test_raw_message_capped_at_4kb(cls: type) -> None:
    big = "X" * 63_000
    e = cls("trunc msg", code=42, raw_message=big)
    assert e.raw_message is not None
    assert len(e.raw_message) < 5000
    assert "raw_message truncated" in e.raw_message


@pytest.mark.parametrize(
    "cls",
    [
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
    ],
)
def test_short_raw_message_round_trips(cls: type) -> None:
    short = "ordinary error"
    e = cls("msg", code=1, raw_message=short)
    assert e.raw_message == short


@pytest.mark.parametrize(
    "cls",
    [InterfaceError, DatabaseError, OperationalError],
)
def test_default_raw_message_from_message_capped(cls: type) -> None:
    """When ``raw_message`` is omitted, ``message`` is the source and is capped."""
    big = "Y" * 63_000
    e = cls(big)
    assert e.raw_message is not None
    assert len(e.raw_message) < 5000
    assert "raw_message truncated" in e.raw_message


def test_database_error_sqlite_errorname_returns_symbolic_name() -> None:
    err = DatabaseError("constraint failed", code=sqlite3.SQLITE_CONSTRAINT_UNIQUE)
    assert err.sqlite_errorname == "SQLITE_CONSTRAINT_UNIQUE"


def test_integrity_error_sqlite_errorname_returns_symbolic_name() -> None:
    err = IntegrityError(
        "constraint failed",
        code=sqlite3.SQLITE_CONSTRAINT_UNIQUE,
        raw_message="x",
    )
    assert err.sqlite_errorname == "SQLITE_CONSTRAINT_UNIQUE"


def test_interface_error_sqlite_errorname_returns_symbolic_name() -> None:
    err = InterfaceError(
        "library misuse",
        code=sqlite3.SQLITE_MISUSE,
        raw_message="x",
    )
    assert err.sqlite_errorname == "SQLITE_MISUSE"


def test_sqlite_errorname_returns_none_for_none_code() -> None:
    err = DatabaseError("no code", code=None)
    assert err.sqlite_errorname is None


def test_sqlite_errorname_returns_none_for_unknown_code() -> None:
    """dqlite-namespace codes (>=1000) have no upstream symbolic name; lookup returns None."""
    err = DatabaseError("dqlite-specific", code=1001)  # DQLITE_PROTO
    assert err.sqlite_errorname is None


@pytest.mark.parametrize(
    "code",
    [
        1002,  # DQLITE_NOTFOUND — collides with stdlib SQLITE_DBCONFIG_ENABLE_FKEY
        1005,  # DQLITE_PARSE — collides with stdlib SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION
    ],
)
def test_sqlite_errorname_none_for_namespace_codes_colliding_with_dbconfig(code: int) -> None:
    """dqlite codes return None even when colliding with stdlib SQLITE_DBCONFIG_* opcodes."""
    assert DatabaseError("namespace", code=code).sqlite_errorname is None


@pytest.mark.parametrize("code", [10250, 10506, 8202, 8458])
def test_sqlite_errorname_none_for_leader_change_codes(code: int) -> None:
    """Leader-change codes return None even though legacy 8202/8458 collide with IOERR codes."""
    assert DatabaseError("leader", code=code).sqlite_errorname is None


def test_module_exports_errorname_alongside_errorcode() -> None:
    assert hasattr(dqlitedbapi.DatabaseError, "sqlite_errorcode")
    assert hasattr(dqlitedbapi.DatabaseError, "sqlite_errorname")
    assert hasattr(dqlitedbapi.InterfaceError, "sqlite_errorcode")
    assert hasattr(dqlitedbapi.InterfaceError, "sqlite_errorname")


@pytest.mark.parametrize(
    ("code", "expected_name"),
    [
        (1, "SQLITE_ERROR"),
        (2, "SQLITE_INTERNAL"),
        (5, "SQLITE_BUSY"),
        (6, "SQLITE_LOCKED"),
        (7, "SQLITE_NOMEM"),
        (8, "SQLITE_READONLY"),
        (10, "SQLITE_IOERR"),
        (11, "SQLITE_CORRUPT"),
        (13, "SQLITE_FULL"),
        (14, "SQLITE_CANTOPEN"),
        (19, "SQLITE_CONSTRAINT"),
        (20, "SQLITE_MISMATCH"),
        (21, "SQLITE_MISUSE"),
        (24, "SQLITE_FORMAT"),
        (25, "SQLITE_RANGE"),
        (26, "SQLITE_NOTADB"),
    ],
)
def test_primary_error_codes_return_canonical_error_names(code: int, expected_name: str) -> None:
    """Primary codes return the error symbol, not an authorizer/opcode constant
    sharing the value (e.g. SQLITE_CREATE_INDEX == SQLITE_ERROR == 1)."""
    err = DatabaseError("test", code=code)
    assert err.sqlite_errorname == expected_name, (
        f"code {code} should yield {expected_name!r}, got {err.sqlite_errorname!r}"
    )


@pytest.fixture
def sync_conn() -> dqlitedbapi.Connection:
    return dqlitedbapi.Connection("localhost:9001")


@pytest.fixture
def async_conn() -> AsyncConnection:
    return AsyncConnection("localhost:9001")


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("iterdump", (), {}),
        ("iterdump", (), {"filter": "*"}),  # stdlib 3.13 added kwarg
        ("iterdump", (), {"filter": "*", "novel_kwarg": True}),
        ("enable_load_extension", (), {}),
        ("enable_load_extension", (True,), {}),
        ("enable_load_extension", (), {"enabled": True}),
        ("enable_load_extension", (True,), {"extra": "ignored"}),
        ("load_extension", (), {"path": "x.so"}),
        ("load_extension", ("x.so",), {}),
        ("load_extension", ("x.so",), {"entrypoint": "init", "extra": 1}),
    ],
)
def test_sync_stub_routes_through_notsupported_error(
    sync_conn: dqlitedbapi.Connection,
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        getattr(sync_conn, method)(*args, **kwargs)


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("iterdump", (), {}),
        ("iterdump", (), {"filter": "*"}),
        ("enable_load_extension", (), {}),
        ("enable_load_extension", (True,), {}),
        ("load_extension", (), {"path": "x.so"}),
        ("load_extension", ("x.so",), {"entrypoint": "init"}),
    ],
)
def test_async_stub_routes_through_notsupported_error(
    async_conn: AsyncConnection,
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        getattr(async_conn, method)(*args, **kwargs)


def test_iterdump_filter_kwarg_does_not_leak_typeerror(
    sync_conn: dqlitedbapi.Connection,
) -> None:
    """iterdump(filter="x") from a 3.13 caller must not leak a bare TypeError."""
    with pytest.raises(dqlitedbapi.NotSupportedError):
        sync_conn.iterdump(filter="x")


def test_enable_load_extension_zero_arg_does_not_leak_typeerror(
    sync_conn: dqlitedbapi.Connection,
) -> None:
    """enable_load_extension() with no args must absorb the call, not leak a TypeError."""
    with pytest.raises(dqlitedbapi.NotSupportedError):
        sync_conn.enable_load_extension()


# dqlite cannot support TPC (Raft is a single-cluster log, no XA coordinator), so the six
# TPC stubs surface NotSupportedError for any signature.


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("tpc_begin", (), {}),
        ("tpc_begin", (object(), object()), {}),
        ("tpc_begin", (object(),), {"format": 1}),
        ("tpc_prepare", (object(),), {}),
        ("tpc_commit", (object(), object()), {}),
        ("tpc_commit", (), {"novel": True}),
        ("tpc_rollback", (), {"novel": True}),
        ("tpc_recover", (), {"timeout": 5}),
        ("xid", (), {}),
        ("xid", (1, "g"), {}),
        ("xid", (1, "g", "b", "extra"), {}),
        ("xid", (1, "g", "b"), {"novel": True}),
    ],
)
def test_sync_tpc_stub_routes_through_notsupported_error(
    sync_conn: dqlitedbapi.Connection,
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        getattr(sync_conn, method)(*args, **kwargs)


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("tpc_begin", (), {}),
        ("tpc_begin", (object(),), {"format": 1}),
        ("tpc_prepare", (object(),), {}),
        ("tpc_commit", (), {"novel": True}),
        ("tpc_rollback", (), {"novel": True}),
        ("tpc_recover", (), {"timeout": 5}),
        ("xid", (), {}),
        ("xid", (1, "g", "b"), {"novel": True}),
    ],
)
def test_async_tpc_stub_routes_through_notsupported_error(
    async_conn: AsyncConnection,
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        getattr(async_conn, method)(*args, **kwargs)
