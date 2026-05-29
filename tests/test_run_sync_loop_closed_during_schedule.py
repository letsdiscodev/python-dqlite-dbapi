"""Pin: a loop closed between ``_ensure_loop`` and ``run_coroutine_threadsafe`` surfaces as a
PEP 249 ``OperationalError`` (so SA's ``is_disconnect`` can classify it), chained from the bare
RuntimeError, and runs ``coro.close()`` so no unawaited-coroutine warning leaks."""

from __future__ import annotations

import asyncio
import warnings
from typing import Any
from unittest.mock import patch

import pytest

import dqlitedbapi.exceptions as _dbapi_exc
from dqlitedbapi.connection import Connection


def _make_connection() -> Connection:
    """Minimal sync Connection with only the state ``_run_sync`` needs."""
    conn = Connection.__new__(Connection)
    import threading

    conn._op_lock = threading.Lock()
    conn._timeout = 5.0
    conn._closed_flag = [False]
    conn._async_conn = None
    conn._creator_pid = 0
    return conn


async def _trivial_coro() -> int:
    return 42


def test_run_sync_raises_pep249_error_when_loop_closed_at_schedule() -> None:
    """A loop closed at schedule time must surface as OperationalError, not bare RuntimeError."""
    conn = _make_connection()
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    with patch.object(conn, "_ensure_loop", return_value=closed_loop):
        with pytest.raises(_dbapi_exc.Error) as exc_info:
            conn._run_sync(_trivial_coro())
        assert isinstance(exc_info.value, _dbapi_exc.OperationalError), (
            f"expected OperationalError, got {type(exc_info.value).__name__}"
        )
        assert isinstance(exc_info.value.__cause__, RuntimeError), (
            f"original RuntimeError must be chained; got {type(exc_info.value.__cause__).__name__}"
        )


def test_run_sync_does_not_leak_unawaited_coroutine_warning() -> None:
    """``coro.close()`` on the schedule-failure path prevents the unawaited-coroutine warning."""
    conn = _make_connection()
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    coro = _trivial_coro()
    with patch.object(conn, "_ensure_loop", return_value=closed_loop):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            with pytest.raises(_dbapi_exc.OperationalError):
                conn._run_sync(coro)
            del coro
            import gc

            gc.collect()
        unawaited = [
            w
            for w in captured
            if issubclass(w.category, RuntimeWarning)
            and "coroutine was never awaited" in str(w.message)
        ]
        assert not unawaited, (
            f"unawaited-coroutine warning leaked from _run_sync's "
            f"schedule-failure path: {[str(w.message) for w in unawaited]}"
        )


def test_run_sync_propagates_runtimeerror_message_in_cause() -> None:
    """The OperationalError must carry the underlying RuntimeError as ``__cause__``."""
    conn = _make_connection()
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    with (
        patch.object(conn, "_ensure_loop", return_value=closed_loop),
        pytest.raises(_dbapi_exc.OperationalError) as exc_info,
    ):
        conn._run_sync(_trivial_coro())
    cause: Any = exc_info.value.__cause__
    assert isinstance(cause, RuntimeError)
    assert str(cause), "RuntimeError cause must carry a non-empty message"
