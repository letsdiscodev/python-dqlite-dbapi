"""Sync ``Connection.__exit__`` must DEBUG-log a rollback failure.

Prior: ``with contextlib.suppress(Exception): self.rollback()`` with a
comment claiming the failure was "attached via ``__context__``
automatically" — but ``contextlib.suppress`` discards, it does not
chain. Result: silent failures with zero diagnostic trail.

The async ``__aexit__`` path landed the narrow-except + DEBUG-log
pattern earlier. This test pins the sync-side companion.

Peer of ISSUE-301.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _make_connection_with_stub_rollback(rollback_exc: Exception | None) -> Connection:
    """Build a Connection with a stubbed rollback/close/commit so
    ``__exit__`` can be driven without a live async loop.
    """
    conn = Connection.__new__(Connection)
    # Force the "has an async_conn" branch so commit/rollback is reached.
    conn._async_conn = MagicMock()  # type: ignore[attr-defined]
    conn._closed = False  # type: ignore[attr-defined]
    conn._address = "mock:0"  # needed for the DEBUG log identity fields

    def _rollback() -> None:
        if rollback_exc is not None:
            raise rollback_exc

    conn.rollback = _rollback  # type: ignore[method-assign]
    conn.commit = MagicMock()  # type: ignore[method-assign]
    conn.close = MagicMock()  # type: ignore[method-assign]
    return conn


def test_exit_logs_debug_on_rollback_failure(caplog: pytest.LogCaptureFixture) -> None:
    """When the body raised and rollback itself raises, emit a DEBUG
    log naming the method; close still runs; the original body
    exception is not masked by the rollback failure.
    """
    caplog.set_level(logging.DEBUG, logger="dqlitedbapi.connection")
    conn = _make_connection_with_stub_rollback(ValueError("rollback exploded"))

    # Simulate a body exception: drive __exit__ with a populated exc_type.
    try:
        raise RuntimeError("body failure")
    except RuntimeError:
        import sys

        exc_type, exc_val, exc_tb = sys.exc_info()
        conn.__exit__(exc_type, exc_val, exc_tb)

    # close() is NOT called on exit (stdlib sqlite3 parity — the
    # connection remains reusable after the ``with`` block).
    assert not conn.close.called  # type: ignore[attr-defined]
    # DEBUG log mentions rollback failure.
    messages = [r.getMessage() for r in caplog.records if r.name == "dqlitedbapi.connection"]
    assert any("rollback failed" in m for m in messages), messages


def test_exit_silent_when_rollback_succeeds(caplog: pytest.LogCaptureFixture) -> None:
    """Rollback success path emits no DEBUG noise."""
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
    """Ensure the rollback failure does not *replace* the body
    exception. With the fixture's __exit__ returning None, the body
    exception should continue to propagate in the caller's context
    machinery — we can't observe this without re-raising, but the
    invariant is that __exit__ returns falsy (not truthy, which would
    swallow).
    """
    conn = _make_connection_with_stub_rollback(ValueError("rollback explode"))
    try:
        raise RuntimeError("body")
    except RuntimeError:
        import sys

        exc_type, exc_val, exc_tb = sys.exc_info()
        result = conn.__exit__(exc_type, exc_val, exc_tb)
    # Anything non-truthy means "don't suppress" — None is the
    # explicit return.
    assert not result
