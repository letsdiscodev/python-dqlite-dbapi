"""Pin: a non-"Event loop is closed" RuntimeError from ``run_coroutine_threadsafe`` propagates
as itself (no OperationalError wrap, which is reserved for the disconnect-shape race so SA's
``is_disconnect`` can classify it) while still running ``coro.close()``."""

from __future__ import annotations

import gc
import threading
import warnings
from unittest.mock import patch

import pytest

from dqlitedbapi.connection import Connection


def _make_connection() -> Connection:
    """Minimal sync Connection with only the state ``_run_sync`` needs."""
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
    """A non-"Event loop is closed" RuntimeError propagates bare, not wrapped."""
    conn = _make_connection()

    err = RuntimeError(
        "Non-thread-safe operation invoked on an event loop other than the current one"
    )
    with (
        patch("asyncio.run_coroutine_threadsafe", side_effect=err),
        patch.object(conn, "_ensure_loop", return_value=object()),
        pytest.raises(RuntimeError, match="Non-thread-safe"),
    ):
        conn._run_sync(_trivial_coro())


def test_run_sync_other_runtimeerror_does_not_leak_unawaited_warning() -> None:
    """``coro.close()`` must run on the non-loop-closed pass-through too, or the warning leaks."""
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
    """The bare ``raise`` must deliver the original message and exception identity verbatim."""
    conn = _make_connection()

    err = RuntimeError("custom programmer-bug message that must reach the caller")
    with (
        patch("asyncio.run_coroutine_threadsafe", side_effect=err),
        patch.object(conn, "_ensure_loop", return_value=object()),
        pytest.raises(RuntimeError) as exc_info,
    ):
        conn._run_sync(_trivial_coro())
    assert "custom programmer-bug message" in str(exc_info.value)
    assert exc_info.value is err
