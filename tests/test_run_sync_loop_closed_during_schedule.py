"""Pin: ``_run_sync`` does not let a bare ``RuntimeError`` from
``asyncio.run_coroutine_threadsafe`` escape the PEP 249 ``Error``
hierarchy, and does not leak the unawaited coroutine.

The race shape: ``_run_sync`` calls ``self._ensure_loop()`` to obtain
a live loop reference, then ``asyncio.run_coroutine_threadsafe(coro,
loop)``. A sibling thread closing the loop between those two calls
(typical: ``do_terminate`` from a finalizer thread, SIGTERM-with-
budget shutdown, manual ``loop.close()`` from a test fixture)
triggers a bare ``RuntimeError`` from ``run_coroutine_threadsafe``.

Bare RuntimeError consequences:

1. Escapes the PEP 249 ``Error`` hierarchy. SA's ``is_disconnect`` is
   gated on ``DatabaseError``; a bare RuntimeError here breaks
   classification and the connection is not retried / invalidated
   correctly.
2. The coroutine's lifecycle is broken — it was never scheduled, and
   nothing called ``coro.close()``. At GC, asyncio emits
   ``RuntimeWarning("coroutine was never awaited")`` that does not
   point at dqlite, sending operators on a wild goose chase.

The fix wraps ``run_coroutine_threadsafe`` in ``try/except
RuntimeError``, calls ``coro.close()`` to suppress the warning, and
raises an ``OperationalError`` (a ``DatabaseError`` subclass that SA's
``is_disconnect`` can classify) chained from the underlying
RuntimeError so the diagnostic is preserved.
"""

from __future__ import annotations

import asyncio
import warnings
from typing import Any
from unittest.mock import patch

import pytest

import dqlitedbapi.exceptions as _dbapi_exc
from dqlitedbapi.connection import Connection


def _make_connection() -> Connection:
    """Build a minimal sync Connection bypassing the constructor's
    cluster machinery; only the state needed for ``_run_sync`` is
    populated.
    """
    conn = Connection.__new__(Connection)
    # Connection's __init__ would normally do this; we only need
    # _op_lock and _timeout for _run_sync's lock-acquire path.
    import threading

    conn._op_lock = threading.Lock()
    conn._timeout = 5.0
    conn._closed_flag = [False]
    conn._async_conn = None  # not needed for the schedule-time race
    conn._creator_pid = 0
    return conn


async def _trivial_coro() -> int:
    return 42


def test_run_sync_raises_pep249_error_when_loop_closed_at_schedule() -> None:
    """A loop closed between ``_ensure_loop`` returning and
    ``run_coroutine_threadsafe`` running must surface as a PEP 249
    ``OperationalError`` (a ``DatabaseError`` subclass), NOT a bare
    ``RuntimeError``.
    """
    conn = _make_connection()
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()  # simulate the race outcome

    with patch.object(conn, "_ensure_loop", return_value=closed_loop):
        with pytest.raises(_dbapi_exc.Error) as exc_info:
            conn._run_sync(_trivial_coro())
        # Specifically OperationalError so SA's is_disconnect can
        # classify it, with the underlying RuntimeError chained on
        # __cause__ for diagnostics.
        assert isinstance(exc_info.value, _dbapi_exc.OperationalError), (
            f"expected OperationalError, got {type(exc_info.value).__name__}"
        )
        assert isinstance(exc_info.value.__cause__, RuntimeError), (
            f"original RuntimeError must be chained; got {type(exc_info.value.__cause__).__name__}"
        )


def test_run_sync_does_not_leak_unawaited_coroutine_warning() -> None:
    """The fix must call ``coro.close()`` on the schedule-failure
    path so asyncio's ``RuntimeWarning("coroutine was never awaited")``
    does NOT fire at GC. Without ``coro.close()`` the warning surfaces
    in caller code with no dqlite frame in the traceback, sending
    operators chasing the wrong layer.
    """
    conn = _make_connection()
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    coro = _trivial_coro()
    with patch.object(conn, "_ensure_loop", return_value=closed_loop):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            with pytest.raises(_dbapi_exc.OperationalError):
                conn._run_sync(coro)
            # Force coroutine GC to surface the warning if it would
            # have fired.
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
    """The OperationalError raised from the schedule-failure path
    must carry the underlying RuntimeError as ``__cause__`` so
    operators reading the traceback see the actual loop-closed signal,
    not just the dqlite-level remap.
    """
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
    # asyncio's wording is "Event loop is closed" — pin a non-empty
    # str(cause) so the diagnostic carries through.
    assert str(cause), "RuntimeError cause must carry a non-empty message"
