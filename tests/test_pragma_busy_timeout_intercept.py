# mypy: disable-error-code="arg-type,type-arg"
"""PRAGMA busy_timeout (setter + getter) is intercepted at the dbapi cursor layer.

dqlite's VFS authorizer denies the PRAGMA server-side, so we handle it client-side and
never reach the wire, mirroring stdlib sqlite3's C-level busy_timeout behaviour.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import NUMBER
from dqlitedbapi._pragma_intercept import try_intercept_busy_timeout
from dqlitewire import ValueType


class _FakeCursor:
    """Minimal cursor stand-in for testing the interception."""

    def __init__(self, busy_timeout_seconds: float = 5.0) -> None:
        self._connection = _FakeConnection(busy_timeout_seconds)
        self._description: tuple | None = None
        self._rows: list = []
        self._rowcount: int = -1
        self._row_index: int = 0


class _FakeConnection:
    def __init__(self, busy_timeout_seconds: float) -> None:
        self._busy_timeout: float = busy_timeout_seconds


def test_setter_eq_form_updates_busy_timeout_and_returns_value() -> None:
    """PRAGMA busy_timeout = N updates the timeout (N is ms, stored as seconds)."""
    cur = _FakeCursor(busy_timeout_seconds=5.0)
    intercepted = try_intercept_busy_timeout(
        cur,
        "PRAGMA busy_timeout = 30000",
        None,
    )
    assert intercepted is True
    assert cur._connection._busy_timeout == 30.0
    assert cur._rows == [(30000,)]
    assert cur._description == (
        ("busy_timeout", int(ValueType.INTEGER), None, None, None, None, None),
    )
    assert cur._rowcount == -1
    assert cur._row_index == 0


def test_busy_timeout_description_type_code_is_integer() -> None:
    """description type_code is the INTEGER wire code so ``== NUMBER`` introspection works."""
    cur = _FakeCursor(busy_timeout_seconds=7.5)
    intercepted = try_intercept_busy_timeout(cur, "PRAGMA busy_timeout", None)
    assert intercepted is True
    assert cur._description is not None
    type_code = cur._description[0][1]
    assert type_code == int(ValueType.INTEGER)
    assert type_code == NUMBER


def test_setter_paren_form_updates_busy_timeout() -> None:
    """PRAGMA busy_timeout(N) is an accepted SQLite alt form."""
    cur = _FakeCursor()
    intercepted = try_intercept_busy_timeout(
        cur,
        "PRAGMA busy_timeout(15000)",
        None,
    )
    assert intercepted is True
    assert cur._connection._busy_timeout == 15.0
    assert cur._rows == [(15000,)]


def test_getter_form_returns_current_value() -> None:
    """PRAGMA busy_timeout (no value) is the getter; it must not modify the value."""
    cur = _FakeCursor(busy_timeout_seconds=7.5)
    intercepted = try_intercept_busy_timeout(
        cur,
        "PRAGMA busy_timeout",
        None,
    )
    assert intercepted is True
    assert cur._connection._busy_timeout == 7.5  # unchanged
    assert cur._rows == [(7500,)]


@pytest.mark.parametrize("ms", [2120321820, 535301320, 65551900])
def test_setter_getter_roundtrip_exact_for_lossy_values(ms: int) -> None:
    """Round-trip must round, not truncate: int() of (N/1000.0)*1000 lands at N-1."""
    cur = _FakeCursor()
    intercepted = try_intercept_busy_timeout(cur, f"PRAGMA busy_timeout = {ms}", None)
    assert intercepted is True
    assert cur._rows == [(ms,)]


def test_case_insensitive() -> None:
    for variant in [
        "pragma busy_timeout = 5000",
        "PRAGMA BUSY_TIMEOUT = 5000",
        "Pragma Busy_Timeout = 5000",
    ]:
        cur = _FakeCursor()
        assert try_intercept_busy_timeout(cur, variant, None) is True, variant


def test_whitespace_tolerant() -> None:
    for variant in [
        "PRAGMA  busy_timeout=5000",
        "  PRAGMA busy_timeout  =  5000  ",
        "PRAGMA\tbusy_timeout\t=\t5000",
        "PRAGMA busy_timeout = 5000;",
    ]:
        cur = _FakeCursor()
        assert try_intercept_busy_timeout(cur, variant, None) is True, variant


def test_negative_value_clamps_to_zero() -> None:
    """SQLite clamps negative busy_timeout to 0."""
    cur = _FakeCursor()
    intercepted = try_intercept_busy_timeout(
        cur,
        "PRAGMA busy_timeout = -100",
        None,
    )
    assert intercepted is True
    assert cur._connection._busy_timeout == 0.0
    assert cur._rows == [(0,)]


def test_zero_value_disables_retry() -> None:
    """PRAGMA busy_timeout = 0 is the 'no retry' setter."""
    cur = _FakeCursor(busy_timeout_seconds=5.0)
    intercepted = try_intercept_busy_timeout(
        cur,
        "PRAGMA busy_timeout = 0",
        None,
    )
    assert intercepted is True
    assert cur._connection._busy_timeout == 0.0
    assert cur._rows == [(0,)]


def test_other_pragmas_not_intercepted() -> None:
    """Regression: only busy_timeout is intercepted; other PRAGMAs flow to the wire."""
    for statement in [
        "PRAGMA foreign_keys = ON",
        "PRAGMA journal_mode",
        "PRAGMA cache_size = 10000",
        "PRAGMA busy_timeoutXXX = 5000",  # name collision check
    ]:
        cur = _FakeCursor()
        assert try_intercept_busy_timeout(cur, statement, None) is False, statement


def test_multi_statement_not_intercepted() -> None:
    """Trailing content after the optional semicolon does not match; routes to wire."""
    cur = _FakeCursor()
    assert (
        try_intercept_busy_timeout(
            cur,
            "PRAGMA busy_timeout = 5000; SELECT 1",
            None,
        )
        is False
    )


def test_non_string_not_intercepted() -> None:
    """A non-str operation returns False rather than crashing the interceptor."""
    cur = _FakeCursor()
    assert try_intercept_busy_timeout(cur, 12345, None) is False
    assert try_intercept_busy_timeout(cur, None, None) is False


@pytest.mark.parametrize(
    "statement",
    [
        "PRAGMA busy_timeout = 5000",
        "PRAGMA busy_timeout=5000",
        "PRAGMA busy_timeout(5000)",
        "PRAGMA busy_timeout(  5000  )",
    ],
)
def test_setter_variants_all_intercept(statement: str) -> None:
    cur = _FakeCursor()
    intercepted = try_intercept_busy_timeout(cur, statement, None)
    assert intercepted is True
    assert cur._connection._busy_timeout == 5.0
