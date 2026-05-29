"""Pin: connect()/Connection() accept, validate, and store a ``busy_timeout`` kwarg.

Default 5.0s (stdlib ``sqlite3.connect(timeout=5.0)`` parity). bool rejected,
non-numeric raises TypeError, negative/non-finite raises ValueError, zero accepted.
"""

from __future__ import annotations

import math

import pytest

from dqlitedbapi import Connection


def _make_conn_no_connect(**kwargs: object) -> Connection:
    """Construct a Connection without dialing; kwarg validation runs in __init__ first."""
    return Connection("localhost:9999", **kwargs)  # type: ignore[arg-type]


def test_connect_accepts_busy_timeout_kwarg() -> None:
    """``dqlitedbapi.connect`` no longer rejects busy_timeout."""
    # Connection() shares the validation path; connect() needs a server.
    conn = _make_conn_no_connect(busy_timeout=10.0)
    assert conn._busy_timeout == 10.0


def test_connect_default_busy_timeout_is_five_seconds() -> None:
    """Stdlib parity: omitted ``busy_timeout`` defaults to 5.0 seconds."""
    conn = _make_conn_no_connect()
    assert conn._busy_timeout == 5.0


def test_connect_busy_timeout_zero_accepted() -> None:
    """``busy_timeout=0`` ("no retry", stdlib parity) is valid, not rejected."""
    conn = _make_conn_no_connect(busy_timeout=0)
    assert conn._busy_timeout == 0.0


def test_connect_busy_timeout_float_zero_accepted() -> None:
    """``busy_timeout=0.0`` is equivalent to ``0``."""
    conn = _make_conn_no_connect(busy_timeout=0.0)
    assert conn._busy_timeout == 0.0


def test_connect_busy_timeout_large_value_accepted() -> None:
    """Large finite values are accepted (no upper cap)."""
    conn = _make_conn_no_connect(busy_timeout=3600.0)
    assert conn._busy_timeout == 3600.0


def test_connect_busy_timeout_rejects_negative() -> None:
    """Negative ``busy_timeout`` is rejected at construction with ValueError."""
    with pytest.raises(ValueError, match="busy_timeout"):
        _make_conn_no_connect(busy_timeout=-1.0)


def test_connect_busy_timeout_rejects_infinity() -> None:
    """Non-finite values break the retry-curve walker; reject."""
    with pytest.raises(ValueError, match="busy_timeout"):
        _make_conn_no_connect(busy_timeout=math.inf)


def test_connect_busy_timeout_rejects_nan() -> None:
    """NaN compares equal to nothing and would bypass the budget check; reject."""
    with pytest.raises(ValueError, match="busy_timeout"):
        _make_conn_no_connect(busy_timeout=math.nan)


def test_connect_busy_timeout_rejects_string() -> None:
    """Non-numeric type raises ``TypeError``."""
    with pytest.raises(TypeError, match="busy_timeout"):
        _make_conn_no_connect(busy_timeout="5.0")


def test_connect_busy_timeout_rejects_none() -> None:
    """``None`` is not a valid timeout; reject."""
    with pytest.raises(TypeError, match="busy_timeout"):
        _make_conn_no_connect(busy_timeout=None)


@pytest.mark.parametrize("bad_value", [True, False])
def test_connect_busy_timeout_rejects_bool(bad_value: bool) -> None:
    """``bool`` is an int to isinstance but would coerce True→1.0/False→0.0; reject."""
    with pytest.raises(TypeError, match="busy_timeout"):
        _make_conn_no_connect(busy_timeout=bad_value)
