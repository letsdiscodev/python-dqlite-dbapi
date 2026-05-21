"""Pin: ``isolation_level`` getter round-trips the setter input, and
both ``autocommit`` and ``isolation_level`` getters surface the
canonical ``InterfaceError("...used after fork...")`` diagnostic when
read from a forked child.

Companion to the round-six widening at ``ca71146``: the setter widened
to accept the stdlib pre-3.12 accept-set, but the getter discarded
the input and always returned ``None``, breaking the canonical
``dst.isolation_level = src.isolation_level`` cross-driver idiom that
the widening was meant to enable.

Companion to the round-six fork-guard sweep at ``76a18ff``: the
sweep enumerated wire-I/O methods and the stub family, but
property getters were excluded — even though the new
``_autocommit_value`` storage (same commit ``ca71146``) introduces
fork-inheritable state. A forked child could read the parent's last
setter input against a dead inner transport.

Both gaps close together because they touch the same four sites
(sync + async, autocommit + isolation_level).
"""

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
    """The widened accept-set round-trips through the getter."""
    conn = Connection("127.0.0.1:9999")
    try:
        conn.isolation_level = value
        assert conn.isolation_level == value
    finally:
        conn._closed = True


def test_sync_isolation_level_default_is_none() -> None:
    """Fresh connection (setter never called) returns ``None`` — the
    documented default for dqlite's autocommit-by-default mode."""
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
    """ProgrammingError on invalid input leaves the previous value
    intact (no partial mutation)."""
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


# --------------------------------------------------------------------
# Fork-guard pins on the autocommit / isolation_level getters.
# Mirrors the canonical guard shape used by ``_stub_unsupported`` /
# ``_ensure_locks``: closed → pid → loop ordering preserved.
# --------------------------------------------------------------------


def _foreign_pid_sync() -> Connection:
    """Build a sync Connection without dialing and simulate a forked
    child by spoofing ``get_current_pid()`` to a different value than
    ``self._creator_pid``."""
    conn = Connection("127.0.0.1:9999")
    return conn


def _foreign_pid_async() -> AsyncConnection:
    aconn = AsyncConnection("127.0.0.1:9999")
    return aconn


def test_sync_autocommit_getter_raises_interface_error_after_fork() -> None:
    """Pin the canonical fork diagnostic on the sync autocommit getter."""
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
    """``closed → pid → loop`` ordering: closed-state diagnostic wins
    over the fork diagnostic, matching the existing discipline."""
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
