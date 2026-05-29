"""``_run_sync`` absorbs up to ``_SYNC_PHASES_MULTIPLIER`` × ``self._timeout`` in its
``Future.result(timeout=...)`` window so each RPC phase gets the full per-phase budget,
matching the async surface which has no cross-RPC ceiling."""

from __future__ import annotations

import asyncio
import time

import pytest

from dqlitedbapi.connection import _SYNC_PHASES_MULTIPLIER, Connection
from dqlitedbapi.exceptions import OperationalError


class TestSyncRunSyncMultiphaseTimeout:
    def test_multiplier_constant_documents_per_phase_count(self) -> None:
        """``_SYNC_PHASES_MULTIPLIER`` matches the worst-case phase count for one high-level
        call (handshake + open + send + read+drain)."""
        assert _SYNC_PHASES_MULTIPLIER == 4
        assert _SYNC_PHASES_MULTIPLIER >= 2, (
            "Steady-state calls have at least 2 phases (send + read+drain)."
        )

    def test_run_sync_window_is_multiplier_times_per_phase_timeout(self) -> None:
        """Wall-clock cap is N × ``self._timeout``, not a single per-phase budget."""
        conn = Connection("localhost:9001", timeout=0.1)
        per_phase = conn._timeout  # 0.1 s
        # ~(N-1) * per_phase: well under N × but over a single window.
        sleep_window = (_SYNC_PHASES_MULTIPLIER - 1) * per_phase

        async def slow_but_legal() -> str:
            await asyncio.sleep(sleep_window)
            return "ok"

        t0 = time.monotonic()
        result = conn._run_sync(slow_but_legal())
        elapsed = time.monotonic() - t0
        assert result == "ok"
        assert elapsed >= sleep_window
        assert elapsed < _SYNC_PHASES_MULTIPLIER * per_phase + 0.5

    def test_run_sync_times_out_after_multiplier_times_per_phase(self) -> None:
        """A coroutine that outlives the full N × budget still raises ``OperationalError``."""
        conn = Connection("localhost:9001", timeout=0.05)

        async def hang_forever() -> None:
            await asyncio.sleep(999)

        t0 = time.monotonic()
        with pytest.raises(OperationalError) as exc_info:
            conn._run_sync(hang_forever())
        elapsed = time.monotonic() - t0

        msg = str(exc_info.value)
        assert elapsed >= _SYNC_PHASES_MULTIPLIER * 0.05 * 0.9, (
            f"Wrapper fired at {elapsed:.3f}s; expected >= {_SYNC_PHASES_MULTIPLIER * 0.05:.3f}s."
        )
        # Message must surface the multiplier so operators don't read
        # "timed out after 0.05s" for a 0.2 s wall-clock.
        assert "timed out" in msg
        assert f"{_SYNC_PHASES_MULTIPLIER}" in msg, (
            f"Timeout error message must surface the multiplier; got {msg!r}"
        )
        assert "per-phase" in msg, (
            f"Timeout error message must explain the multiplier rationale; got {msg!r}"
        )

    def test_timeout_message_exposes_total_window_seconds(self) -> None:
        """The reported timeout window is the N × per-phase total (wall-clock budget)."""
        conn = Connection("localhost:9001", timeout=0.05)
        expected_total = _SYNC_PHASES_MULTIPLIER * 0.05

        async def hang_forever() -> None:
            await asyncio.sleep(999)

        with pytest.raises(OperationalError) as exc_info:
            conn._run_sync(hang_forever())

        msg = str(exc_info.value)
        # Both total and per-phase appear so the arithmetic is auditable from the string alone.
        assert f"{expected_total}" in msg or f"{expected_total:.2f}" in msg, (
            f"Total window must appear in timeout message; got {msg!r}"
        )
        assert "0.05" in msg, (
            f"Per-phase budget must appear in timeout message for auditability; got {msg!r}"
        )
