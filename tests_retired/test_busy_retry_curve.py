"""Pin: ``next_busy_delay_ms`` matches SQLite's ``sqliteDefaultBusyCallback`` curve exactly."""

from __future__ import annotations

import pytest

from dqlitedbapi._busy_retry import next_busy_delay_ms

# Canonical SQLite curve from sqliteDefaultBusyCallback.
_EXPECTED_DELAYS = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100]


def test_curve_first_twelve_attempts_match_sqlite() -> None:
    """First 12 attempts return the canonical SQLite delays when the budget won't clamp."""
    big_budget_ms = 60_000
    for count, expected_delay in enumerate(_EXPECTED_DELAYS):
        actual = next_busy_delay_ms(count, big_budget_ms)
        assert actual == expected_delay, (
            f"attempt #{count}: expected {expected_delay}ms, got {actual}ms"
        )


def test_curve_after_twelve_attempts_flattens_at_100ms() -> None:
    """After the explicit delays array is exhausted, the C library sleeps 100ms per retry."""
    big_budget_ms = 60_000
    for count in (12, 13, 50, 100, 500):
        assert next_busy_delay_ms(count, big_budget_ms) == 100


def test_zero_budget_returns_none_at_count_zero() -> None:
    """``busy_timeout_ms=0`` (stdlib's "no wait") returns None even at count=0."""
    assert next_busy_delay_ms(0, 0) is None


def test_negative_budget_returns_none() -> None:
    """Negative budgets short-circuit like zero (defensive; validators reject them upstream)."""
    assert next_busy_delay_ms(0, -1) is None
    assert next_busy_delay_ms(5, -1000) is None


def test_budget_exhausted_returns_none() -> None:
    """When clamped ``delay <= 0`` the helper returns None. Note ``prior`` reads the fixed
    ``_BUSY_TOTALS_MS[count]`` table, not a runtime cumulative."""
    assert next_busy_delay_ms(2, 5) == 2
    assert next_busy_delay_ms(3, 5) is None


def test_budget_exactly_at_curve_boundary() -> None:
    """Budget equal to a cumulative-total step: when prior == budget, delay clamps to 0 → None."""
    assert next_busy_delay_ms(1, 3) == 2
    assert next_busy_delay_ms(2, 3) is None


@pytest.mark.parametrize(
    "count,budget,expected",
    [
        (0, 5000, 1),
        (11, 5000, 100),
        (12, 1000, 100),
        (6, 100, 25),
        (7, 100, 22),  # clamped: prior=78, delay=25 → 103 overruns, 100-78=22
        (8, 100, None),  # prior=103 > 100
    ],
)
def test_curve_parametrised(count: int, budget: int, expected: int | None) -> None:
    """Catch off-by-one regressions in the curve walker across budget × count combinations."""
    assert next_busy_delay_ms(count, budget) == expected


def test_curve_internally_consistent() -> None:
    """Pin ``totals[i+1] == totals[i] + delays[i]`` against accidental edits to the tables."""
    from dqlitedbapi._busy_retry import _BUSY_DELAYS_MS, _BUSY_TOTALS_MS

    assert len(_BUSY_DELAYS_MS) == len(_BUSY_TOTALS_MS)
    for i in range(len(_BUSY_DELAYS_MS) - 1):
        expected = _BUSY_TOTALS_MS[i] + _BUSY_DELAYS_MS[i]
        actual = _BUSY_TOTALS_MS[i + 1]
        assert expected == actual, (
            f"totals[{i + 1}] inconsistent: totals[{i}]={_BUSY_TOTALS_MS[i]} "
            f"+ delays[{i}]={_BUSY_DELAYS_MS[i]} = {expected}, table has {actual}"
        )
