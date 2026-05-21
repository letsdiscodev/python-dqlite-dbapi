"""``Connection.close()`` joins the loop thread within
``self._close_timeout`` (floored at ``_LOOP_THREAD_JOIN_MIN_SECONDS``),
mirroring ``force_close_transport()``.

The graceful close used to join with a hard-coded 5 s constant, ignoring
the operator's ``close_timeout`` knob and breaking both directions:

- Tight-budget operators (SIGTERM SLOs with ``close_timeout=0.5``)
  saw graceful close pay 5 s on a stuck loop.
- WAN-tuned operators (``close_timeout=10``) saw the join truncated
  at 5 s — the configured value silently did nothing.

These tests pin both directions, plus a symmetry check that the
graceful and force-close paths consult the same source of truth.
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.connection import (
    _LOOP_THREAD_JOIN_MIN_SECONDS,
    Connection,
)


@contextlib.contextmanager
def _patched_dqlite_connection():
    """Patch the leader-discovery + DqliteConnection construction so a
    Connection can spin up its loop+thread without touching the wire."""
    with (
        patch(
            "dqlitedbapi.connection._resolve_leader",
            new=AsyncMock(side_effect=lambda address, **_kw: address),
        ),
        patch("dqlitedbapi.connection.DqliteConnection") as MockDqliteConn,
    ):
        instance = AsyncMock()
        instance.connect = AsyncMock()
        instance._protocol = AsyncMock()
        instance._db_id = 0
        instance._in_use = False
        instance._pending_drain = None
        MockDqliteConn.return_value = instance
        yield instance


def _establish_loop(conn: Connection) -> None:
    """Spin up the background loop + thread without actually connecting."""
    with _patched_dqlite_connection():
        conn._run_sync(conn._get_async_connection())


class _JoinTimeoutRecorder:
    """Wrap a ``threading.Thread`` so ``join(timeout=...)`` arguments are
    captured for inspection while still forwarding to the real join."""

    def __init__(self, thread: threading.Thread) -> None:
        self._thread = thread
        self.timeouts: list[float | None] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._thread, name)

    def join(self, timeout: float | None = None) -> None:
        self.timeouts.append(timeout)
        self._thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        return self._thread.is_alive()


class TestCloseThreadJoinUsesCloseTimeout:
    def test_close_thread_join_uses_close_timeout(self) -> None:
        """Graceful ``close()`` derives its thread.join timeout from
        ``self._close_timeout`` (floored at the documented MIN), NOT
        from a hard-coded constant."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.25)
        _establish_loop(conn)
        recorder = _JoinTimeoutRecorder(conn._thread)  # type: ignore[arg-type]
        conn._thread = recorder  # type: ignore[assignment]

        conn.close()

        assert len(recorder.timeouts) == 1
        observed = recorder.timeouts[0]
        # close_timeout=0.25 is above the floor; budget == 0.25.
        assert observed == pytest.approx(0.25, abs=1e-9), (
            f"join timeout was {observed!r}; expected close_timeout=0.25 (no hard-coded constant)."
        )

    def test_close_thread_join_extends_for_wan_close_timeout(self) -> None:
        """A WAN-tuned ``close_timeout`` past the prior 5 s constant
        extends the graceful-close join budget — the configured value
        is no longer silently truncated."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=12.5)
        _establish_loop(conn)
        recorder = _JoinTimeoutRecorder(conn._thread)  # type: ignore[arg-type]
        conn._thread = recorder  # type: ignore[assignment]

        conn.close()

        observed = recorder.timeouts[0]
        assert observed == pytest.approx(12.5, abs=1e-9), (
            f"join timeout was {observed!r}; expected close_timeout=12.5, "
            "not the prior 5 s ceiling."
        )

    def test_close_thread_join_floor_applies_at_min(self) -> None:
        """``close_timeout`` below the documented floor is widened to
        ``_LOOP_THREAD_JOIN_MIN_SECONDS`` — the queued ``loop.stop``
        needs enough scheduling slack to land on a non-stuck loop."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.01)
        _establish_loop(conn)
        recorder = _JoinTimeoutRecorder(conn._thread)  # type: ignore[arg-type]
        conn._thread = recorder  # type: ignore[assignment]

        conn.close()

        observed = recorder.timeouts[0]
        assert observed == _LOOP_THREAD_JOIN_MIN_SECONDS, (
            f"join timeout was {observed!r}; expected the floor "
            f"({_LOOP_THREAD_JOIN_MIN_SECONDS}) to widen close_timeout=0.01."
        )

    def test_force_close_thread_join_uses_close_timeout(self) -> None:
        """``force_close_transport()`` join budget mirrors ``close()``:
        operator's ``close_timeout`` floored at MIN."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.4)
        _establish_loop(conn)
        recorder = _JoinTimeoutRecorder(conn._thread)  # type: ignore[arg-type]
        conn._thread = recorder  # type: ignore[assignment]

        conn.force_close_transport()

        observed = recorder.timeouts[0]
        assert observed == pytest.approx(0.4, abs=1e-9)

    def test_close_and_force_close_share_join_budget_derivation(self) -> None:
        """Pin symmetry: both close shapes feed the same value through
        the join timeout for the same ``close_timeout`` setting."""
        budgets: dict[str, float | None] = {}
        for shape in ("close", "force_close_transport"):
            conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.3)
            _establish_loop(conn)
            recorder = _JoinTimeoutRecorder(conn._thread)  # type: ignore[arg-type]
            conn._thread = recorder  # type: ignore[assignment]
            getattr(conn, shape)()
            budgets[shape] = recorder.timeouts[0]

        assert budgets["close"] == budgets["force_close_transport"], (
            f"Asymmetric join budgets across close paths: {budgets!r}"
        )


@pytest.mark.parametrize("close_timeout", [0.1, 0.5, 2.0])
def test_finalizer_captures_close_timeout(close_timeout: float) -> None:
    """The ``weakref.finalize``-registered cleanup mirrors the operator's
    ``close_timeout`` so the GC path is not held to a 5 s legacy
    constant either."""
    conn = Connection("localhost:19001", timeout=10.0, close_timeout=close_timeout)
    _establish_loop(conn)
    finalizer = conn._finalizer
    assert finalizer is not None
    # peek() returns ``(obj, func, args_tuple, kwargs_dict)``. The
    # positional args in the finalize registration are
    # ``(loop, thread, closed_flag, address, creator_pid,
    # close_timeout, inner_finalize_handle)`` — close_timeout is
    # at index 5 from the start; the inner_finalize_handle box
    # follows it.
    peeked = finalizer.peek()
    assert peeked is not None
    _obj, _func, args, _kwargs = peeked
    # Look up by argspec position rather than args[-1] so a future
    # addition of another positional after the inner_finalize_handle
    # does not silently silence this pin.
    captured_close_timeout = args[5]
    assert captured_close_timeout == close_timeout, (
        f"finalizer captured close_timeout={captured_close_timeout!r}; expected {close_timeout!r}"
    )
    conn.close()
