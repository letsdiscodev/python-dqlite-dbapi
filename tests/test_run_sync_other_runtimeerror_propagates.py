"""Pin: ``_run_sync`` propagates a non-"Event loop is closed" bare
``RuntimeError`` from ``asyncio.run_coroutine_threadsafe`` AS ITSELF
(no wrap into ``OperationalError``), and still runs ``coro.close()``
on the way out so the unawaited-coroutine warning does not fire.

Companion to ``test_run_sync_loop_closed_during_schedule`` which pins
the OTHER branch (loop-closed → OperationalError). The wrap is
narrowly reserved for transient DB-shape failures so SA's
``is_disconnect`` can classify them; programmer-bug RuntimeErrors
(e.g. "Non-thread-safe operation invoked on an event loop other than
the current one") must surface as themselves — otherwise they are
silently routed through retry logic and masked behind benign-looking
reconnect noise.
"""

from __future__ import annotations

import gc
import threading
import warnings
from unittest.mock import patch

import pytest

from dqlitedbapi.connection import Connection


def _make_connection() -> Connection:
    """Minimal Connection bypassing the constructor's cluster
    machinery; only the state needed for ``_run_sync`` is populated."""
    conn = Connection.__new__(Connection)
    conn._op_lock = threading.Lock()
    conn._timeout = 5.0
    conn._closed_flag = [False]
    conn._async_conn = None
    conn._creator_pid = 0
    return conn


async def _trivial_coro() -> int:
    return 42


def test_run_sync_propagates_non_loop_closed_runtimeerror_verbatim() -> None:
    """A RuntimeError whose message does NOT contain
    "Event loop is closed" must propagate as a bare ``RuntimeError``,
    NOT be wrapped in ``OperationalError``. The narrow remap is
    reserved for the disconnect-shape race; programmer-bug
    RuntimeErrors are not database failures."""
    conn = _make_connection()

    err = RuntimeError(
        "Non-thread-safe operation invoked on an event loop other than the current one"
    )
    with (
        patch("asyncio.run_coroutine_threadsafe", side_effect=err),
        # _ensure_loop must succeed; any loop object works since
        # run_coroutine_threadsafe is patched to raise immediately.
        patch.object(conn, "_ensure_loop", return_value=object()),
        pytest.raises(RuntimeError, match="Non-thread-safe"),
    ):
        conn._run_sync(_trivial_coro())


def test_run_sync_other_runtimeerror_does_not_leak_unawaited_warning() -> None:
    """``coro.close()`` must run on EVERY RuntimeError arm — including
    the non-loop-closed pass-through. Without it the unawaited-
    coroutine warning fires at GC in caller code with no dqlite frame
    in the traceback."""
    conn = _make_connection()

    coro = _trivial_coro()
    err = RuntimeError("Non-thread-safe operation invoked")
    with (
        patch("asyncio.run_coroutine_threadsafe", side_effect=err),
        patch.object(conn, "_ensure_loop", return_value=object()),
    ):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            with pytest.raises(RuntimeError, match="Non-thread-safe"):
                conn._run_sync(coro)
            del coro
            gc.collect()
        unawaited = [
            w
            for w in captured
            if issubclass(w.category, RuntimeWarning)
            and "coroutine was never awaited" in str(w.message)
        ]
        assert not unawaited, (
            f"unawaited-coroutine warning leaked from _run_sync's "
            f"non-loop-closed RuntimeError arm: "
            f"{[str(w.message) for w in unawaited]}"
        )


def test_run_sync_other_runtimeerror_preserves_message() -> None:
    """The pass-through is a bare ``raise``: the original message and
    type must reach the caller verbatim so an operator triaging the
    programmer bug sees the original signal."""
    conn = _make_connection()

    err = RuntimeError("custom programmer-bug message that must reach the caller")
    with (
        patch("asyncio.run_coroutine_threadsafe", side_effect=err),
        patch.object(conn, "_ensure_loop", return_value=object()),
        pytest.raises(RuntimeError) as exc_info,
    ):
        conn._run_sync(_trivial_coro())
    assert "custom programmer-bug message" in str(exc_info.value)
    # Bare ``raise`` preserves identity — exactly the original
    # exception object, no remap.
    assert exc_info.value is err
