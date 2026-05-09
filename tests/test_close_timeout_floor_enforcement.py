"""Pin: ``_validate_close_timeout`` enforces the 0.01 s floor at the
dbapi layer so direct callers see the same diagnostic SA's URL parser
enforces.

Threat model: a programmer using ``dqlitedbapi.aio.aconnect(addr,
close_timeout=0.0001)`` directly bypasses SA's URL-side floor. Below
0.01 s, the dispose-time writer-close completes before FIN flushes,
leaving connections lingering in TIME_WAIT. The SA URL parser
previously enforced this floor at the dialect boundary; centralising
the check at the dbapi layer means every entry path (URL,
connect_args, direct dbapi, direct client) sees the same diagnostic.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import ProgrammingError
from dqlitedbapi.connection import _validate_close_timeout


def test_close_timeout_below_floor_raises_programming_error() -> None:
    with pytest.raises(ProgrammingError, match="close_timeout must be >= 0.01"):
        _validate_close_timeout(0.0001)


def test_close_timeout_at_floor_accepted() -> None:
    _validate_close_timeout(0.01)  # boundary value


def test_close_timeout_at_default_accepted() -> None:
    _validate_close_timeout(0.5)  # default


def test_close_timeout_zero_rejected_via_validate_timeout() -> None:
    """Zero is rejected by the inner ``validate_timeout`` (positive
    requirement) BEFORE the floor check fires; the diagnostic
    mentions positivity."""
    with pytest.raises(ProgrammingError, match="positive"):
        _validate_close_timeout(0.0)


def test_close_timeout_negative_rejected() -> None:
    with pytest.raises(ProgrammingError, match="positive"):
        _validate_close_timeout(-0.5)
