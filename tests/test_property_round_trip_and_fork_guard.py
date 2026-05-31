"""Pin: ``isolation_level`` getter round-trips the setter input, and both
``autocommit`` and ``isolation_level`` getters raise the fork diagnostic in a child."""

from __future__ import annotations

import contextlib
import os
import sqlite3
from unittest.mock import patch

import pytest

import dqlitedbapi
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.parametrize(
    "value",
    [None, "", "DEFERRED", "IMMEDIATE", "EXCLUSIVE", "deferred", "Immediate"],
)
def test_sync_isolation_level_round_trips_setter_input(value: object) -> None:
    """The widened accept-set round-trips through the getter, uppercased like stdlib."""
    conn = Connection("127.0.0.1:9999")
    try:
        conn.isolation_level = value
        expected = value.upper() if isinstance(value, str) else value
        assert conn.isolation_level == expected
    finally:
        conn._closed = True


def test_sync_isolation_level_default_is_none() -> None:
    """Fresh connection returns ``None`` — dqlite is autocommit-by-default."""
    conn = Connection("127.0.0.1:9999")
    try:
        assert conn.isolation_level is None
    finally:
        conn._closed = True


def test_sync_isolation_level_stdlib_round_trip_idiom() -> None:
    """The canonical cross-driver
    ``dst.isolation_level = src.isolation_level`` idiom round-trips."""
    src = sqlite3.connect(":memory:")
    try:
        dst = Connection("127.0.0.1:9999")
        try:
            dst.isolation_level = src.isolation_level  # stdlib default ""
            assert dst.isolation_level == src.isolation_level
            assert dst.isolation_level == ""
        finally:
            dst._closed = True
    finally:
        src.close()


def test_sync_isolation_level_invalid_setter_input_preserves_prior_value() -> None:
    """Invalid input raises ProgrammingError and leaves the previous value intact."""
    conn = Connection("127.0.0.1:9999")
    try:
        conn.isolation_level = "DEFERRED"
        with pytest.raises(dqlitedbapi.ProgrammingError):
            conn.isolation_level = "SERIALIZABLE"
        assert conn.isolation_level == "DEFERRED"
    finally:
        conn._closed = True


def test_async_isolation_level_round_trips_setter_input() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        aconn.isolation_level = "DEFERRED"
        assert aconn.isolation_level == "DEFERRED"
        aconn.isolation_level = None
        assert aconn.isolation_level is None
    finally:
        aconn._closed = True


def test_async_isolation_level_default_is_none() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        assert aconn.isolation_level is None
    finally:
        aconn._closed = True


def _foreign_pid_sync() -> Connection:
    conn = Connection("127.0.0.1:9999")
    return conn


def _foreign_pid_async() -> AsyncConnection:
    aconn = AsyncConnection("127.0.0.1:9999")
    return aconn


def test_sync_autocommit_getter_raises_interface_error_after_fork() -> None:
    conn = _foreign_pid_sync()
    try:
        with (
            patch(
                "dqlitedbapi.connection.get_current_pid",
                return_value=conn._creator_pid + 1,
            ),
            pytest.raises(InterfaceError, match="used after fork"),
        ):
            _ = conn.autocommit
    finally:
        conn._closed = True


def test_sync_isolation_level_getter_raises_interface_error_after_fork() -> None:
    conn = _foreign_pid_sync()
    try:
        with (
            patch(
                "dqlitedbapi.connection.get_current_pid",
                return_value=conn._creator_pid + 1,
            ),
            pytest.raises(InterfaceError, match="used after fork"),
        ):
            _ = conn.isolation_level
    finally:
        conn._closed = True


def test_async_autocommit_getter_raises_interface_error_after_fork() -> None:
    aconn = _foreign_pid_async()
    try:
        with (
            patch(
                "dqlitedbapi.aio.connection.get_current_pid",
                return_value=aconn._creator_pid + 1,
            ),
            pytest.raises(InterfaceError, match="used after fork"),
        ):
            _ = aconn.autocommit
    finally:
        aconn._closed = True


def test_async_isolation_level_getter_raises_interface_error_after_fork() -> None:
    aconn = _foreign_pid_async()
    try:
        with (
            patch(
                "dqlitedbapi.aio.connection.get_current_pid",
                return_value=aconn._creator_pid + 1,
            ),
            pytest.raises(InterfaceError, match="used after fork"),
        ):
            _ = aconn.isolation_level
    finally:
        aconn._closed = True


def test_sync_autocommit_getter_closed_state_precedence_survives() -> None:
    """Closed-state diagnostic wins over the fork diagnostic (closed -> pid -> loop)."""
    conn = _foreign_pid_sync()
    conn._closed = True
    with (
        patch(
            "dqlitedbapi.connection.get_current_pid",
            return_value=conn._creator_pid + 1,
        ),
        pytest.raises(InterfaceError, match="Connection is closed"),
    ):
        _ = conn.autocommit


def test_sync_isolation_level_getter_closed_state_precedence_survives() -> None:
    conn = _foreign_pid_sync()
    conn._closed = True
    with (
        patch(
            "dqlitedbapi.connection.get_current_pid",
            return_value=conn._creator_pid + 1,
        ),
        pytest.raises(InterfaceError, match="Connection is closed"),
    ):
        _ = conn.isolation_level


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_sync_isolation_level_getter_after_real_fork_raises() -> None:
    """End-to-end: real fork + read in the child surfaces InterfaceError."""
    conn = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        conn.isolation_level = "DEFERRED"
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                os.close(r)
                try:
                    _ = conn.isolation_level
                    os.write(w, b"NO_RAISE")
                except InterfaceError as e:
                    msg = str(e)
                    if "fork" in msg and "reconstruct from configuration" in msg:
                        os.write(w, b"OK")
                    else:
                        os.write(w, f"WRONG_MSG:{msg}".encode())
                except Exception as e:  # noqa: BLE001
                    os.write(w, f"WRONG_TYPE:{type(e).__name__}:{e}".encode())
                finally:
                    os.close(w)
            finally:
                os._exit(0)
        os.close(w)
        result = b""
        with contextlib.suppress(OSError):
            while chunk := os.read(r, 4096):
                result += chunk
        os.close(r)
        os.waitpid(pid, 0)
        assert result == b"OK", result.decode(errors="replace")
    finally:
        with contextlib.suppress(Exception):
            conn._closed = True
