"""``_run_sync`` honours the documented per-RPC-phase contract by
absorbing up to ``_SYNC_PHASES_MULTIPLIER`` × ``self._timeout`` in the
``Future.result(timeout=...)`` window — matching the async surface,
which wraps each individual RPC in ``asyncio.timeout(self._timeout)``
and exposes no cross-RPC ceiling.

Previously the sync wrapper bounded the entire multi-phase coroutine
at a single ``self._timeout`` window, collapsing N phases into one
budget and silently violating the contract documented on the
``timeout`` kwarg ("Each phase of an operation [...] gets the full
budget independently — a single call can take up to roughly N ×
``timeout`` end-to-end"). The result was false-positive timeouts on
slow-but-healthy peers and a sync vs. async surface drift that broke
cross-dialect porting.

These tests pin:
- the new ``Future.result`` window is ``N × self._timeout``,
- the ``OperationalError`` message exposes the multiplier so operators
  don't see ``"timed out after 5s"`` when wall-clock was 20 s,
- the multiplier matches the documented worst-case phase count.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from dqlitedbapi.connection import _SYNC_PHASES_MULTIPLIER, Connection
from dqlitedbapi.exceptions import OperationalError


class TestSyncRunSyncMultiphaseTimeout:
    def test_multiplier_constant_documents_per_phase_count(self) -> None:
        """``_SYNC_PHASES_MULTIPLIER`` matches the worst-case phase
        count for a single high-level sync call (handshake + open +
        send + read+drain)."""
        # Pin the documented N=4. If a future protocol change adds /
        # removes a phase, this assertion catches the constant drift
        # at the same place that documents the phase enumeration.
        assert _SYNC_PHASES_MULTIPLIER == 4
        assert _SYNC_PHASES_MULTIPLIER >= 2, (
            "Steady-state calls have at least 2 phases (send + read+drain)."
        )

    def test_run_sync_window_is_multiplier_times_per_phase_timeout(self) -> None:
        """Wall-clock cap on the sync wrapper is N × ``self._timeout``,
        not a single per-phase budget."""
        # Use a small per-phase timeout so the test runs quickly. The
        # async coroutine sleeps for slightly less than the N × budget
        # — the sync wrapper must NOT time out (each phase fits inside
        # the per-phase budget; total wall-clock is just under N ×).
        conn = Connection("localhost:9001", timeout=0.1)
        per_phase = conn._timeout  # 0.1 s
        # Sleep for ~ (N-1) * per_phase — still well under N × per_phase
        # but well over a single per_phase window. Pre-fix, the wrapper
        # would have fired its single-window cap at ~per_phase.
        sleep_window = (_SYNC_PHASES_MULTIPLIER - 1) * per_phase

        async def slow_but_legal() -> str:
            await asyncio.sleep(sleep_window)
            return "ok"

        t0 = time.monotonic()
        result = conn._run_sync(slow_but_legal())
        elapsed = time.monotonic() - t0
        assert result == "ok"
        # Must succeed (no timeout) — the multi-phase budget absorbed
        # the (N-1) × per_phase sleep.
        assert elapsed >= sleep_window
        # Sanity upper bound — must be below the N × budget plus the
        # 1 s cancel-wait safety net the wrapper adds on timeout.
        assert elapsed < _SYNC_PHASES_MULTIPLIER * per_phase + 0.5

    def test_run_sync_times_out_after_multiplier_times_per_phase(self) -> None:
        """A coroutine that genuinely outlives the full N × budget
        still raises ``OperationalError`` — the contract is N × per
        phase, not unbounded."""
        # Per-phase 0.05 → N × = 0.2 → cancel-wait adds up to 1 s on top.
        conn = Connection("localhost:9001", timeout=0.05)

        async def hang_forever() -> None:
            await asyncio.sleep(999)

        t0 = time.monotonic()
        with pytest.raises(OperationalError) as exc_info:
            conn._run_sync(hang_forever())
        elapsed = time.monotonic() - t0

        msg = str(exc_info.value)
        # The wrapper must wait at least N × per-phase before firing.
        assert elapsed >= _SYNC_PHASES_MULTIPLIER * 0.05 * 0.9, (
            f"Wrapper fired at {elapsed:.3f}s; expected >= {_SYNC_PHASES_MULTIPLIER * 0.05:.3f}s."
        )
        # And the error message MUST surface the multiplier so operators
        # don't see ``"timed out after 0.05s"`` for a 0.2 s wall-clock.
        assert "timed out" in msg
        assert f"{_SYNC_PHASES_MULTIPLIER}" in msg, (
            f"Timeout error message must surface the multiplier; got {msg!r}"
        )
        assert "per-phase" in msg, (
            f"Timeout error message must explain the multiplier rationale; got {msg!r}"
        )

    def test_timeout_message_exposes_total_window_seconds(self) -> None:
        """The reported timeout window MUST be the N × per-phase total
        so operators reading logs see the wall-clock budget, not the
        per-phase knob."""
        conn = Connection("localhost:9001", timeout=0.05)
        expected_total = _SYNC_PHASES_MULTIPLIER * 0.05

        async def hang_forever() -> None:
            await asyncio.sleep(999)

        with pytest.raises(OperationalError) as exc_info:
            conn._run_sync(hang_forever())

        msg = str(exc_info.value)
        # Both the total AND the per-phase value should appear so the
        # arithmetic is auditable from the error string alone.
        assert f"{expected_total}" in msg or f"{expected_total:.2f}" in msg, (
            f"Total window must appear in timeout message; got {msg!r}"
        )
        assert "0.05" in msg, (
            f"Per-phase budget must appear in timeout message for auditability; got {msg!r}"
        )
