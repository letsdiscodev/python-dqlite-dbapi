"""Pin: ``_busy_retry.next_busy_delay_ms`` matches SQLite's
``main.c::sqliteDefaultBusyCallback`` curve exactly.

This is the deterministic retry curve stdlib ``sqlite3`` uses when
``busy_timeout > 0``:

    delays = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100]
    totals = [0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228]

(Verified internally consistent: totals[i+1] == totals[i] + delays[i].)

After exhausting ``delays``, the C library sleeps 100 ms per attempt
until the cumulative sleep ≥ ``busy_timeout_ms``, then returns BUSY
to the caller. The Python helper mirrors this one-to-one so the
retry shape is identical to stdlib.

Also pins:

- ``busy_timeout_ms <= 0`` short-circuits ``None`` (stdlib parity
  for ``timeout=0`` = "no wait").
- The final-step clamp: when ``prior + delay`` would overrun the
  budget, ``delay`` is shortened to fit; if the clamp would yield
  ``delay <= 0`` the helper returns ``None`` (caller raises original
  BUSY).
"""

from __future__ import annotations

import pytest

from dqlitedbapi._busy_retry import next_busy_delay_ms

# The canonical SQLite curve (extracted from sqliteDefaultBusyCallback).
_EXPECTED_DELAYS = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100]


def test_curve_first_twelve_attempts_match_sqlite() -> None:
    """First 12 attempts (the explicit ``delays`` array) return the
    canonical SQLite delays when the budget is large enough not to
    clamp."""
    big_budget_ms = 60_000  # 60s — far larger than the curve's 328ms total
    for count, expected_delay in enumerate(_EXPECTED_DELAYS):
        actual = next_busy_delay_ms(count, big_budget_ms)
        assert actual == expected_delay, (
            f"attempt #{count}: expected {expected_delay}ms, got {actual}ms"
        )


def test_curve_after_twelve_attempts_flattens_at_100ms() -> None:
    """After the explicit ``delays`` array is exhausted, the C library
    sleeps 100ms per retry. Pin the same shape."""
    big_budget_ms = 60_000
    for count in (12, 13, 50, 100, 500):
        assert next_busy_delay_ms(count, big_budget_ms) == 100


def test_zero_budget_returns_none_at_count_zero() -> None:
    """``busy_timeout_ms=0`` = stdlib's ``timeout=0`` = "no wait".
    Even at count=0 the helper returns None so the caller raises BUSY
    immediately."""
    assert next_busy_delay_ms(0, 0) is None


def test_negative_budget_returns_none() -> None:
    """Negative budgets short-circuit the same way as zero. This is a
    defensive belt-and-suspenders pin — the validators upstream
    reject negatives at construction, but the helper must remain
    safe if a bug ever permits one through."""
    assert next_busy_delay_ms(0, -1) is None
    assert next_busy_delay_ms(5, -1000) is None


def test_budget_exhausted_returns_none() -> None:
    """When cumulative ``prior + delay`` would overrun the budget AND
    the clamp would yield ``delay <= 0``, the helper returns None.

    With budget=5ms: the curve says attempt 0 sleeps 1ms (total 1ms),
    attempt 1 sleeps 2ms (total 3ms), attempt 2 sleeps 5ms (total 8ms
    — overruns). The clamp shortens attempt 2 to ``5 - 3 = 2``ms (still
    positive → return 2). Attempt 3 would then have prior=5ms (after
    the clamped 2ms sleep) — no, actually ``totals`` is the C curve's
    fixed table; the helper uses ``_BUSY_TOTALS_MS[count]`` not the
    runtime cumulative — so attempt 3 reads prior=8ms which already
    exceeds budget=5ms → ``5 - 8 = -3 → None``.
    """
    # Attempt 2 with budget=5: curve says prior=3, delay=5. Clamped
    # to 5-3=2 → returns 2.
    assert next_busy_delay_ms(2, 5) == 2
    # Attempt 3 with budget=5: curve says prior=8 — already past
    # budget → None.
    assert next_busy_delay_ms(3, 5) is None


def test_budget_exactly_at_curve_boundary() -> None:
    """Edge case: budget equal to the cumulative-total at a curve
    step. The next attempt would have ``prior + delay`` strictly
    greater than budget (since delay > 0), so clamp computes
    ``delay = budget - prior``. If prior == budget, delay clamps to
    0 → None.

    ``_BUSY_TOTALS_MS[1] = 3`` (cumulative after attempt 0). So
    budget=3, attempt 1: prior=1, delay=2 → prior+delay=3 (not
    greater than 3) → returns 2 unchanged.
    Then attempt 2: prior=3 (=budget), delay would clamp to 0 → None.
    """
    assert next_busy_delay_ms(1, 3) == 2
    assert next_busy_delay_ms(2, 3) is None


@pytest.mark.parametrize(
    "count,budget,expected",
    [
        # Stdlib-default 5000ms budget: full curve runs cleanly.
        (0, 5000, 1),
        (11, 5000, 100),
        # 1000ms budget: tail at 100ms ramps until prior >= 1000.
        # After count=11, prior=228; subsequent attempts add 100 each.
        # Attempt 12: prior=228+100*(12-11)=328, delay=100 → sum 428 → ok.
        (12, 1000, 100),
        # 100ms budget: very tight. Count=0..5 all fit (cumulative
        # 1+2+5+10+15+20=53), count=6 prior=53, delay=25 → 78 fits.
        (6, 100, 25),
        # Count=7 prior=78, delay=25 → 103 overruns; clamp 100-78=22.
        (7, 100, 22),
        # Count=8 prior=103 > 100 → None.
        (8, 100, None),
    ],
)
def test_curve_parametrised(count: int, budget: int, expected: int | None) -> None:
    """Parametrised pin across several budget × count combinations to
    catch off-by-one regressions in the curve walker."""
    assert next_busy_delay_ms(count, budget) == expected


def test_curve_internally_consistent() -> None:
    """Sanity: ``totals[i+1] == totals[i] + delays[i]`` across the
    entire curve. Pin against accidental edits to ``_BUSY_DELAYS_MS``
    or ``_BUSY_TOTALS_MS`` (e.g. dropping or duplicating the second
    25 in a row)."""
    from dqlitedbapi._busy_retry import _BUSY_DELAYS_MS, _BUSY_TOTALS_MS

    assert len(_BUSY_DELAYS_MS) == len(_BUSY_TOTALS_MS)
    for i in range(len(_BUSY_DELAYS_MS) - 1):
        expected = _BUSY_TOTALS_MS[i] + _BUSY_DELAYS_MS[i]
        actual = _BUSY_TOTALS_MS[i + 1]
        assert expected == actual, (
            f"totals[{i + 1}] inconsistent: totals[{i}]={_BUSY_TOTALS_MS[i]} "
            f"+ delays[{i}]={_BUSY_DELAYS_MS[i]} = {expected}, table has {actual}"
        )
