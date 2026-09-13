"""Connection: connect kwargs, address/timeout validation, autocommit/isolation, shortcuts, repr."""

from __future__ import annotations

import contextlib
import inspect
import sqlite3
import threading
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi import Connection, ProgrammingError
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError, NotSupportedError


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "no-port",
        "host:",
        "host:abc",
        "host:0",
        "host:70000",
        "::1:9000",  # unbracketed IPv6
    ],
)
def test_sync_init_rejects_invalid_address(bad: str) -> None:
    with pytest.raises(ProgrammingError, match="Invalid address"):
        Connection(bad)


def test_sync_init_rejects_non_string_address() -> None:
    with pytest.raises(ProgrammingError, match="host:port"):
        Connection(None)  # type: ignore[arg-type]


def test_sync_init_accepts_valid_address() -> None:
    Connection("127.0.0.1:9001")


def test_async_init_rejects_invalid_address() -> None:
    with pytest.raises(ProgrammingError, match="Invalid address"):
        AsyncConnection("host:abc")


def test_async_init_rejects_non_string_address() -> None:
    with pytest.raises(ProgrammingError, match="host:port"):
        AsyncConnection(None)  # type: ignore[arg-type]


def test_async_init_accepts_valid_address() -> None:
    AsyncConnection("[::1]:9001")


_BAD_TIMEOUTS = (
    -1.0,
    0.0,
    float("inf"),
    float("-inf"),
    float("nan"),
)


@pytest.mark.parametrize("timeout", _BAD_TIMEOUTS)
def test_sync_connect_rejects_bad_timeout(timeout: float) -> None:
    with pytest.raises(ProgrammingError, match="timeout must be a positive finite number"):
        dqlitedbapi.connect("localhost:9001", timeout=timeout)


@pytest.mark.parametrize("timeout", _BAD_TIMEOUTS)
def test_aio_connect_rejects_bad_timeout(timeout: float) -> None:
    with pytest.raises(ProgrammingError, match="timeout must be a positive finite number"):
        dqlitedbapi.aio.connect("localhost:9001", timeout=timeout)


@pytest.mark.parametrize("timeout", _BAD_TIMEOUTS)
async def test_aio_aconnect_rejects_bad_timeout(timeout: float) -> None:
    with pytest.raises(ProgrammingError, match="timeout must be a positive finite number"):
        await dqlitedbapi.aio.aconnect("localhost:9001", timeout=timeout)


def test_error_phrasing_includes_value() -> None:
    """The error repeats the offending value so operators can spot typos without the callsite."""
    with pytest.raises(ProgrammingError) as excinfo:
        dqlitedbapi.connect("localhost:9001", timeout=-3.5)
    assert "-3.5" in str(excinfo.value)


@pytest.mark.parametrize("bad", [True, False])
def test_sync_connect_rejects_bool_timeout(bad: bool) -> None:
    """bool subclasses int, so without an isinstance guard ``timeout=True`` silently gives 1s."""
    with pytest.raises(ProgrammingError, match="bool"):
        dqlitedbapi.connect("localhost:9001", timeout=bad)


@pytest.mark.parametrize("bad", [True, False])
def test_sync_connect_rejects_bool_close_timeout(bad: bool) -> None:
    with pytest.raises(ProgrammingError, match="bool"):
        dqlitedbapi.connect("localhost:9001", close_timeout=bad)


@pytest.mark.parametrize("bad", [True, False])
async def test_aio_aconnect_rejects_bool_timeout(bad: bool) -> None:
    with pytest.raises(ProgrammingError, match="bool"):
        await dqlitedbapi.aio.aconnect("localhost:9001", timeout=bad)


def test_sync_connect_accepts_isolation_level_none() -> None:
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


def test_sync_connect_accepts_autocommit_true() -> None:
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=True)
    _ctor.assert_called_once()


def test_sync_connect_accepts_autocommit_minus_one() -> None:
    """Stdlib's ``LEGACY_TRANSACTION_CONTROL`` sentinel (-1) is an accepted no-op."""
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=-1)
    _ctor.assert_called_once()


def test_sync_connect_rejects_isolation_level_unknown_string() -> None:
    """Unknown strings stay rejected even though the setter accepts the stdlib set."""
    with pytest.raises(ProgrammingError, match="isolation_level"):
        dqlitedbapi.connect("127.0.0.1:9001", isolation_level="SERIALIZABLE")


def test_sync_connect_rejects_autocommit_false() -> None:
    with pytest.raises(NotSupportedError, match="autocommit"):
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=False)


def test_async_connect_accepts_isolation_level_none() -> None:
    from dqlitedbapi.aio import connect as aio_connect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        aio_connect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


def test_async_connect_rejects_isolation_level_unknown_string() -> None:
    from dqlitedbapi.aio import connect as aio_connect

    with pytest.raises(ProgrammingError, match="isolation_level"):
        aio_connect("127.0.0.1:9001", isolation_level="SERIALIZABLE")


@pytest.mark.asyncio
async def test_aconnect_accepts_isolation_level_none() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_accepts_autocommit_true() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", autocommit=True)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_accepts_autocommit_minus_one() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", autocommit=-1)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_rejects_isolation_level_unknown_string() -> None:
    from dqlitedbapi.aio import aconnect

    with pytest.raises(ProgrammingError, match="isolation_level"):
        await aconnect("127.0.0.1:9001", isolation_level="SERIALIZABLE")


@pytest.mark.asyncio
async def test_aconnect_rejects_autocommit_false() -> None:
    from dqlitedbapi.aio import aconnect

    with pytest.raises(NotSupportedError, match="autocommit"):
        await aconnect("127.0.0.1:9001", autocommit=False)


@pytest.mark.parametrize(
    "value",
    [None, "", "DEFERRED", "IMMEDIATE", "EXCLUSIVE", "deferred", "Immediate"],
)
def test_sync_isolation_level_accepts_stdlib_value(value: object) -> None:
    """Stdlib values are accepted (case-insensitive for the string variants)."""
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        conn.isolation_level = value
    finally:
        conn.close()


def test_sync_isolation_level_rejects_unknown_string_as_programming_error() -> None:
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            conn.isolation_level = "SERIALIZABLE"
    finally:
        conn.close()


def test_sync_isolation_level_rejects_integer_as_programming_error() -> None:
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            conn.isolation_level = 42
    finally:
        conn.close()


def test_sync_isolation_level_stdlib_round_trip_idiom() -> None:
    """``dst.isolation_level = src.isolation_level`` works against a stdlib
    source."""
    import sqlite3

    src = sqlite3.connect(":memory:")
    try:
        dst = dqlitedbapi.Connection("127.0.0.1:9999")
        try:
            dst.isolation_level = src.isolation_level  # stdlib default is ""
        finally:
            dst.close()
    finally:
        src.close()


def test_async_isolation_level_accepts_stdlib_default_empty_string() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        aconn.isolation_level = ""
    finally:
        aconn.force_close_transport()


def test_async_isolation_level_rejects_unknown_string_as_programming_error() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            aconn.isolation_level = "SERIALIZABLE"
    finally:
        aconn.force_close_transport()


def test_sync_autocommit_accepts_true() -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = True
    finally:
        conn.close()


def test_sync_autocommit_accepts_legacy_transaction_control_sentinel() -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL
    finally:
        conn.close()


@pytest.mark.parametrize("value", [False, 0, 1, "yes", []])
def test_sync_autocommit_rejects_non_true_non_sentinel(value: object) -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        with pytest.raises(NotSupportedError, match="autocommit"):
            conn.autocommit = value
    finally:
        conn.close()


async def test_async_autocommit_accepts_legacy_transaction_control_sentinel() -> None:
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    aconn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL


@pytest.mark.parametrize("value", [False, 0, 1, "yes"])
async def test_async_autocommit_rejects_non_true_non_sentinel(value: object) -> None:
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    with pytest.raises(NotSupportedError, match="autocommit"):
        aconn.autocommit = value


def test_module_exports_legacy_transaction_control_sentinel_via_stdlib() -> None:
    """Sanity: the sentinel value is the stdlib symbol we accept."""
    assert sqlite3.LEGACY_TRANSACTION_CONTROL == -1
    assert dqlitedbapi.apilevel == "2.0"


# Nothing listens on port 1: any dial attempt would raise OperationalError.
UNREACHABLE = "127.0.0.1:1"


def test_commit_on_unused_connection_is_noop() -> None:
    conn = dqlitedbapi.connect(UNREACHABLE, timeout=2.0)
    conn.commit()
    conn.close()


def test_rollback_on_unused_connection_is_noop() -> None:
    conn = dqlitedbapi.connect(UNREACHABLE, timeout=2.0)
    conn.rollback()
    conn.close()


def test_connect_on_closed_from_creator_thread_raises_interface_error() -> None:
    """On the creator thread, closed-state surfaces as InterfaceError."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn.close()
    try:
        conn.connect()
    except InterfaceError as e:
        assert "closed" in str(e).lower()
    else:
        raise AssertionError("expected InterfaceError on closed connect()")


def test_connect_from_other_thread_raises_thread_affinity_first() -> None:
    """Foreign-thread caller gets the thread-affinity ProgrammingError even when
    closed: the thread check runs before the closed-state check."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn.close()

    result_holder: list[Any] = [None]

    def call_from_other_thread() -> None:
        try:
            conn.connect()
        except Exception as e:  # noqa: BLE001
            result_holder[0] = e

    t = threading.Thread(target=call_from_other_thread)
    t.start()
    t.join(timeout=2.0)

    assert isinstance(result_holder[0], ProgrammingError), (
        f"thread-affinity precedence broken: expected ProgrammingError "
        f"from a foreign-thread caller (even on closed conn), got "
        f"{type(result_holder[0]).__name__}: {result_holder[0]}"
    )


def test_connect_from_other_thread_on_open_conn_raises_thread_affinity() -> None:
    """Open conn from a foreign thread still raises the thread-affinity diagnostic."""
    conn = Connection("localhost:9001", timeout=2.0)
    try:
        result_holder: list[Any] = [None]

        def call_from_other_thread() -> None:
            try:
                conn.connect()
            except Exception as e:  # noqa: BLE001
                result_holder[0] = e

        t = threading.Thread(target=call_from_other_thread)
        t.start()
        t.join(timeout=2.0)
        assert isinstance(result_holder[0], ProgrammingError)
    finally:
        conn.close()


def test_sync_connection_class_exposes_cursor_attribute() -> None:
    assert getattr(dqlitedbapi.Connection, "Cursor") is Cursor  # noqa: B009


def test_async_connection_class_exposes_cursor_attribute() -> None:
    assert getattr(AsyncConnection, "AsyncCursor") is AsyncCursor  # noqa: B009


def test_sync_connection_instance_attribute_routes_to_class_attribute() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    assert getattr(conn, "Cursor") is Cursor  # noqa: B009


def test_async_connection_instance_attribute_routes_to_class_attribute() -> None:
    aconn = AsyncConnection("localhost:9001")
    assert getattr(aconn, "AsyncCursor") is AsyncCursor  # noqa: B009


def test_async_connection_class_exposes_cursor_attribute_aiosqlite_shape() -> None:
    """aiosqlite-shape parity: also expose the async cursor under ``Connection.Cursor``
    so ``isinstance(cur, conn.Cursor)`` works against our ``AsyncConnection``."""
    assert getattr(AsyncConnection, "Cursor") is AsyncCursor  # noqa: B009


def test_async_connection_instance_cursor_attribute_aiosqlite_shape() -> None:
    aconn = AsyncConnection("localhost:9001")
    assert getattr(aconn, "Cursor") is AsyncCursor  # noqa: B009


def test_sync_cursor_rejects_factory_kwarg() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        with pytest.raises(NotSupportedError, match="factory"):
            conn.cursor(factory=object)
    finally:
        conn.close()


def test_sync_cursor_rejects_arbitrary_unknown_kwarg() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        with pytest.raises(NotSupportedError):
            conn.cursor(unknown=True)
    finally:
        conn.close()


def test_sync_cursor_no_kwargs_still_works() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        cur = conn.cursor()
        assert cur is not None
        cur.close()
    finally:
        conn.close()


async def test_async_cursor_rejects_factory_kwarg() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(NotSupportedError, match="factory"):
        aconn.cursor(factory=object)


async def test_async_cursor_no_kwargs_still_works() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    cur = aconn.cursor()
    assert cur is not None


def test_sync_connection_has_execute_method() -> None:
    assert hasattr(dqlitedbapi.Connection, "execute"), (
        "dqlitedbapi.Connection should expose execute() — stdlib "
        "sqlite3.Connection.execute, AsyncAdaptedConnection.execute "
        "and SA's reference connector all have it."
    )


def test_sync_connection_execute_returns_cursor_and_calls_through() -> None:
    """execute() opens a cursor, forwards to ``cur.execute(...)``, and returns the cursor."""
    conn = Connection("localhost:9001", timeout=1.0)
    fake_cur = MagicMock()
    conn.cursor = MagicMock(return_value=fake_cur)

    result = conn.execute("SELECT 1")

    assert result is fake_cur
    fake_cur.execute.assert_called_once_with("SELECT 1")
    fake_cur.close.assert_not_called()


def test_sync_connection_execute_passes_parameters() -> None:
    conn = Connection("localhost:9001", timeout=1.0)
    fake_cur = MagicMock()
    conn.cursor = MagicMock(return_value=fake_cur)

    conn.execute("SELECT ?", [1])

    fake_cur.execute.assert_called_once_with("SELECT ?", [1])


def test_sync_connection_execute_closes_cursor_on_synchronous_raise() -> None:
    """A synchronous ``cur.execute(...)`` failure must close the cursor before re-raising."""
    conn = Connection("localhost:9001", timeout=1.0)
    fake_cur = MagicMock()
    fake_cur.execute.side_effect = RuntimeError("simulated execute failure")
    conn.cursor = MagicMock(return_value=fake_cur)

    with pytest.raises(RuntimeError, match="simulated execute failure"):
        conn.execute("SELECT 1")

    fake_cur.close.assert_called_once_with()


def test_sync_connection_has_executemany_shortcut() -> None:
    assert hasattr(Connection, "executemany")
    assert callable(Connection.executemany)


def test_sync_executemany_signature_matches_async() -> None:
    """Sync parameters must match the async sibling so swapping surfaces keeps the shape."""
    sync_params = list(inspect.signature(Connection.executemany).parameters.keys())
    async_params = list(inspect.signature(AsyncConnection.executemany).parameters.keys())
    assert sync_params == async_params


def test_sync_executemany_returns_cursor_annotation() -> None:
    sig = inspect.signature(Connection.executemany)
    assert sig.return_annotation is Cursor


def test_module_connect_signature_unchanged() -> None:
    assert callable(dqlitedbapi.connect)


def _exec(conn: dqlitedbapi.Connection, sql: str) -> None:
    """Run ``sql`` on a fresh cursor, closed before return to avoid a ResourceWarning."""
    with conn.cursor() as cur:
        cur.execute(sql)


async def _aexec(conn: AsyncConnection, sql: str) -> None:
    """Async sibling of :func:`_exec`."""
    cur = conn.cursor()
    try:
        await cur.execute(sql)
    finally:
        cur.close()


def test_sync_executemany_rejects_str_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_outer")
        _exec(conn, "CREATE TABLE exm_outer (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer VALUES (?)", "abc")
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_outer")
        conn.close()


def test_sync_executemany_rejects_dict_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_outer2")
        _exec(conn, "CREATE TABLE exm_outer2 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer2 VALUES (?)", {"a": 1})
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_outer2")
        conn.close()


def test_sync_executemany_rejects_bytes_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_outer3")
        _exec(conn, "CREATE TABLE exm_outer3 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer3 VALUES (?)", b"abc")  # type: ignore[arg-type]
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_outer3")
        conn.close()


def test_sync_executemany_rejects_set_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_set")
        _exec(conn, "CREATE TABLE exm_set (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_set VALUES (?)", {(1,), (2,)})
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_set")
        conn.close()


def test_sync_executemany_rejects_frozenset_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_fset")
        _exec(conn, "CREATE TABLE exm_fset (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany(
                "INSERT INTO exm_fset VALUES (?)",
                frozenset({(1,), (2,)}),
            )
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_fset")
        conn.close()


def test_sync_executemany_accepts_list_of_tuples() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_ok")
        _exec(conn, "CREATE TABLE exm_ok (n INTEGER)")
        cur = conn.executemany("INSERT INTO exm_ok VALUES (?)", [(1,), (2,)])
        cur.close()
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_ok")
        conn.close()


async def test_async_executemany_rejects_str_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer")
        await _aexec(conn, "CREATE TABLE aexm_outer (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer VALUES (?)", "abc")
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer")
        await conn.close()


async def test_async_executemany_rejects_dict_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer2")
        await _aexec(conn, "CREATE TABLE aexm_outer2 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer2 VALUES (?)", {"a": 1})
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer2")
        await conn.close()


async def test_async_executemany_rejects_memoryview_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer3")
        await _aexec(conn, "CREATE TABLE aexm_outer3 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer3 VALUES (?)", memoryview(b"abc"))
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer3")
        await conn.close()


async def test_async_executemany_rejects_set_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_set")
        await _aexec(conn, "CREATE TABLE aexm_set (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany(
                "INSERT INTO aexm_set VALUES (?)",
                {(1,), (2,)},
            )
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_set")
        await conn.close()


async def test_async_executemany_rejects_frozenset_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_fset")
        await _aexec(conn, "CREATE TABLE aexm_fset (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany(
                "INSERT INTO aexm_fset VALUES (?)",
                frozenset({(1,), (2,)}),
            )
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_fset")
        await conn.close()


async def test_async_executemany_accepts_list_of_tuples() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_ok")
        await _aexec(conn, "CREATE TABLE aexm_ok (n INTEGER)")
        cur2 = await conn.executemany("INSERT INTO aexm_ok VALUES (?)", [(1,), (2,)])
        cur2.close()
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_ok")
        await conn.close()


class TestConnectionRepr:
    def test_connection_repr_includes_address(self) -> None:
        conn = Connection("localhost:19001", database="x", timeout=2.0)
        try:
            r = repr(conn)
            assert "Connection" in r
            assert "localhost:19001" in r
            assert not r.startswith("<dqlitedbapi.connection.Connection object at ")
        finally:
            conn.close()

    def test_async_connection_repr(self) -> None:
        conn = AsyncConnection("localhost:19001", database="x")
        r = repr(conn)
        assert "AsyncConnection" in r
        assert "localhost:19001" in r


class TestCursorRepr:
    def test_cursor_repr(self) -> None:
        conn = Connection("localhost:19001", timeout=2.0)
        try:
            c = conn.cursor()
            r = repr(c)
            assert "Cursor" in r
            assert "rowcount" in r
        finally:
            conn.close()

    def test_async_cursor_repr(self) -> None:
        conn = AsyncConnection("localhost:19001")
        c = AsyncCursor(conn)
        r = repr(c)
        assert "AsyncCursor" in r
        assert "rowcount" in r

    def test_cursor_repr_state_transition(self) -> None:
        """Repr reports ``open`` before close and ``closed`` after."""
        conn = Connection("localhost:19001", timeout=2.0)
        try:
            c = conn.cursor()
            assert "open" in repr(c)
            c.close()
            assert "closed" in repr(c)
        finally:
            conn.close()

    def test_async_cursor_repr_state_transition(self) -> None:
        conn = AsyncConnection("localhost:19001")
        c = AsyncCursor(conn)
        assert "open" in repr(c)
        # ``AsyncCursor.close`` is sync by design (see its docstring).
        c.close()
        assert "closed" in repr(c)
