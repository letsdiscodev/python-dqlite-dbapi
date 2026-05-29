"""Pin: ``retry_sync_on_busy`` implements the SQLite-curve BUSY retry (stdlib parity)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi._busy_retry import retry_sync_on_busy
from dqlitedbapi.exceptions import OperationalError
from dqlitewire import SQLITE_BUSY


def test_retry_sync_succeeds_after_busy_then_ok() -> None:
    """First call raises BUSY, second returns: helper sleeps once and retries."""
    run_sync_mock = MagicMock(
        side_effect=[OperationalError("database is locked", code=SQLITE_BUSY), "ok"]
    )
    with patch("dqlitedbapi._busy_retry.time.sleep") as sleep_mock:
        result = retry_sync_on_busy(5.0, run_sync_mock, lambda: MagicMock())
    assert result == "ok"
    assert run_sync_mock.call_count == 2
    sleep_mock.assert_called_once_with(0.001)


def test_retry_sync_sleeps_sqlite_curve() -> None:
    """Sleep durations follow the SQLite curve (1, 2, 5, 10, 15 ms), not exponential."""
    run_sync_mock = MagicMock(
        side_effect=[
            OperationalError("locked", code=SQLITE_BUSY),
            OperationalError("locked", code=SQLITE_BUSY),
            OperationalError("locked", code=SQLITE_BUSY),
            OperationalError("locked", code=SQLITE_BUSY),
            OperationalError("locked", code=SQLITE_BUSY),
            "ok",
        ]
    )
    with patch("dqlitedbapi._busy_retry.time.sleep") as sleep_mock:
        result = retry_sync_on_busy(5.0, run_sync_mock, lambda: MagicMock())
    assert result == "ok"
    actual_sleeps_ms = [int(call.args[0] * 1000) for call in sleep_mock.call_args_list]
    assert actual_sleeps_ms == [1, 2, 5, 10, 15]


def test_retry_sync_raises_when_budget_exhausted() -> None:
    """When cumulative sleep would exceed the budget, raise the original OperationalError."""
    busy_exc = OperationalError("locked", code=SQLITE_BUSY)
    run_sync_mock = MagicMock(side_effect=busy_exc)
    with (
        patch("dqlitedbapi._busy_retry.time.sleep"),
        pytest.raises(OperationalError) as exc_info,
    ):
        retry_sync_on_busy(0.005, run_sync_mock, lambda: MagicMock())  # 5ms budget
    assert exc_info.value.code == SQLITE_BUSY


def test_retry_sync_non_busy_propagates_immediately() -> None:
    """A different OperationalError code (SQLITE_LOCKED=6) is raised immediately, no sleep."""
    run_sync_mock = MagicMock(side_effect=OperationalError("locked", code=6))
    with (
        patch("dqlitedbapi._busy_retry.time.sleep") as sleep_mock,
        pytest.raises(OperationalError) as exc_info,
    ):
        retry_sync_on_busy(5.0, run_sync_mock, lambda: MagicMock())
    assert exc_info.value.code == 6
    sleep_mock.assert_not_called()
    assert run_sync_mock.call_count == 1


def test_retry_sync_zero_budget_no_retry() -> None:
    """``busy_timeout=0`` → no retry: first BUSY raises immediately, no sleep."""
    run_sync_mock = MagicMock(side_effect=OperationalError("locked", code=SQLITE_BUSY))
    with (
        patch("dqlitedbapi._busy_retry.time.sleep") as sleep_mock,
        pytest.raises(OperationalError),
    ):
        retry_sync_on_busy(0.0, run_sync_mock, lambda: MagicMock())
    sleep_mock.assert_not_called()
    assert run_sync_mock.call_count == 1


def test_retry_sync_keyboardinterrupt_propagates() -> None:
    """KeyboardInterrupt (a BaseException) must not be caught by the OperationalError arm."""
    run_sync_mock = MagicMock(side_effect=OperationalError("locked", code=SQLITE_BUSY))
    with (
        patch("dqlitedbapi._busy_retry.time.sleep", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        retry_sync_on_busy(5.0, run_sync_mock, lambda: MagicMock())


def test_retry_sync_coro_factory_called_each_attempt() -> None:
    """Coroutines are single-use, so the helper calls ``coro_factory()`` once per attempt."""
    run_sync_mock = MagicMock(
        side_effect=[
            OperationalError("locked", code=SQLITE_BUSY),
            OperationalError("locked", code=SQLITE_BUSY),
            "ok",
        ]
    )
    coro_factory_calls: list[Any] = []

    def factory() -> Any:
        sentinel = MagicMock(name=f"coro_{len(coro_factory_calls)}")
        coro_factory_calls.append(sentinel)
        return sentinel

    with patch("dqlitedbapi._busy_retry.time.sleep"):
        retry_sync_on_busy(5.0, run_sync_mock, factory)
    assert len(coro_factory_calls) == 3
