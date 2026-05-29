"""_validate_close_timeout enforces the 0.01s floor at the dbapi layer so every entry path
(URL, connect_args, direct dbapi/client) sees the same diagnostic. Below 0.01s, the
dispose-time writer-close completes before FIN flushes, leaving connections in TIME_WAIT.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import ProgrammingError
from dqlitedbapi.connection import _validate_close_timeout


def test_close_timeout_below_floor_raises_programming_error() -> None:
    with pytest.raises(ProgrammingError, match="close_timeout must be >= 0.01"):
        _validate_close_timeout(0.0001)


def test_close_timeout_floor_diagnostic_carries_fin_flush_rationale() -> None:
    """The wrapper threads min_value_rationale to validate_timeout so the FIN-flush
    explanation matches what direct client callers see when the floor trips."""
    with pytest.raises(ProgrammingError) as exc:
        _validate_close_timeout(0.0001)
    assert "FIN flushes" in str(exc.value), (
        "_validate_close_timeout must forward the FIN-flush rationale "
        "to the client layer's validate_timeout so dbapi-layer and "
        "SA-URL operators see the same operator-facing explanation."
    )


def test_close_timeout_at_floor_accepted() -> None:
    _validate_close_timeout(0.01)


def test_close_timeout_at_default_accepted() -> None:
    _validate_close_timeout(0.5)


def test_close_timeout_zero_rejected_via_validate_timeout() -> None:
    """Zero is rejected by validate_timeout's positivity check before the floor check fires."""
    with pytest.raises(ProgrammingError, match="positive"):
        _validate_close_timeout(0.0)


def test_close_timeout_negative_rejected() -> None:
    with pytest.raises(ProgrammingError, match="positive"):
        _validate_close_timeout(-0.5)
