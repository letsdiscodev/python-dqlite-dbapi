"""close() joins the loop thread within self._close_timeout (floored at
_LOOP_THREAD_JOIN_MIN_SECONDS), mirroring force_close_transport(). The prior hard-coded 5s
ignored the operator's close_timeout knob in both directions (tight-budget and WAN-tuned).
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
    """Patch leader-discovery + DqliteConnection so a Connection spins up loop+thread offline."""
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
    """Spin up the background loop + thread without connecting."""
    with _patched_dqlite_connection():
        conn._run_sync(conn._get_async_connection())


class _JoinTimeoutRecorder:
    """Wrap a Thread to capture join(timeout=...) arguments while forwarding to the real join."""

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
        """Graceful close() derives its thread.join timeout from self._close_timeout."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.25)
        _establish_loop(conn)
        recorder = _JoinTimeoutRecorder(conn._thread)  # type: ignore[arg-type]
        conn._thread = recorder  # type: ignore[assignment]

        conn.close()

        assert len(recorder.timeouts) == 1
        observed = recorder.timeouts[0]
        assert observed == pytest.approx(0.25, abs=1e-9), (
            f"join timeout was {observed!r}; expected close_timeout=0.25 (no hard-coded constant)."
        )

    def test_close_thread_join_extends_for_wan_close_timeout(self) -> None:
        """A WAN-tuned close_timeout past the prior 5s constant extends the join budget."""
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
        """close_timeout below the floor is widened to _LOOP_THREAD_JOIN_MIN_SECONDS so the
        queued loop.stop has enough scheduling slack to land on a non-stuck loop."""
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
        """force_close_transport() join budget mirrors close(): close_timeout floored at MIN."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.4)
        _establish_loop(conn)
        recorder = _JoinTimeoutRecorder(conn._thread)  # type: ignore[arg-type]
        conn._thread = recorder  # type: ignore[assignment]

        conn.force_close_transport()

        observed = recorder.timeouts[0]
        assert observed == pytest.approx(0.4, abs=1e-9)

    def test_close_and_force_close_share_join_budget_derivation(self) -> None:
        """Both close shapes feed the same join timeout for the same close_timeout setting."""
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
    """The weakref.finalize cleanup mirrors close_timeout so the GC path is not held to 5s."""
    conn = Connection("localhost:19001", timeout=10.0, close_timeout=close_timeout)
    _establish_loop(conn)
    finalizer = conn._finalizer
    assert finalizer is not None
    # peek() -> (obj, func, args, kwargs); close_timeout is positional arg index 5.
    peeked = finalizer.peek()
    assert peeked is not None
    _obj, _func, args, _kwargs = peeked
    # Index by argspec position, not args[-1], so a future trailing positional
    # does not silently silence this pin.
    captured_close_timeout = args[5]
    assert captured_close_timeout == close_timeout, (
        f"finalizer captured close_timeout={captured_close_timeout!r}; expected {close_timeout!r}"
    )
    conn.close()
