"""Sync ``Connection.__exit__`` DEBUG-logs a rollback failure (``suppress`` discards it)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _make_connection_with_stub_rollback(rollback_exc: Exception | None) -> Connection:
    """Build a Connection with stubbed rollback/close/commit, no live async loop."""
    conn = Connection.__new__(Connection)
    # Force the "has an async_conn" branch so commit/rollback is reached.
    conn._async_conn = MagicMock()
    conn._closed = False
    conn._address = "mock:0"  # needed for the DEBUG log identity fields

    def _rollback() -> None:
        if rollback_exc is not None:
            raise rollback_exc

    conn.rollback = _rollback
    conn.commit = MagicMock()
    conn.close = MagicMock()
    return conn


def test_exit_logs_debug_on_rollback_failure(caplog: pytest.LogCaptureFixture) -> None:
    """When the body raised and rollback also raises, emit a DEBUG log; body exc not masked."""
    caplog.set_level(logging.DEBUG, logger="dqlitedbapi.connection")
    conn = _make_connection_with_stub_rollback(ValueError("rollback exploded"))

    try:
        raise RuntimeError("body failure")
    except RuntimeError:
        import sys

        exc_type, exc_val, exc_tb = sys.exc_info()
        conn.__exit__(exc_type, exc_val, exc_tb)

    # close() is NOT called on exit (stdlib sqlite3 parity: connection stays reusable).
    assert not conn.close.called  # type: ignore[attr-defined]
    messages = [r.getMessage() for r in caplog.records if r.name == "dqlitedbapi.connection"]
    assert any("rollback failed" in m for m in messages), messages


def test_exit_silent_when_rollback_succeeds(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="dqlitedbapi.connection")
    conn = _make_connection_with_stub_rollback(None)

    try:
        raise RuntimeError("body failure")
    except RuntimeError:
        import sys

        exc_type, exc_val, exc_tb = sys.exc_info()
        conn.__exit__(exc_type, exc_val, exc_tb)

    messages = [r.getMessage() for r in caplog.records if r.name == "dqlitedbapi.connection"]
    assert not any("rollback failed" in m for m in messages), messages


def test_exit_does_not_mask_body_exception_path() -> None:
    """The rollback failure must not replace the body exception: __exit__ returns falsy."""
    conn = _make_connection_with_stub_rollback(ValueError("rollback explode"))
    try:
        raise RuntimeError("body")
    except RuntimeError:
        import sys

        exc_type, exc_val, exc_tb = sys.exc_info()
        result = conn.__exit__(exc_type, exc_val, exc_tb)  # type: ignore[func-returns-value]
    assert not result
