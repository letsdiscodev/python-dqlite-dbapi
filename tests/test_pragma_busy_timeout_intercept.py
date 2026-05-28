# mypy: disable-error-code="arg-type,type-arg"
"""Pin: ``PRAGMA busy_timeout`` (setter + getter) is intercepted at
the dbapi cursor layer.

dqlite's VFS authorizer denies the PRAGMA server-side; the
interception lets callers tune the busy_timeout via the canonical
SQLite escape hatch without it ever reaching the wire. Mirrors
stdlib ``sqlite3``'s C-level behaviour where the PRAGMA writes the
connection's busy_timeout register and returns the new value.

Behaviours pinned:

- Setter form (``PRAGMA busy_timeout = N``) updates
  ``connection._busy_timeout`` (seconds, divided from ms) and
  emits the new value as a single-row result.
- Setter alt form (``PRAGMA busy_timeout(N)``) works the same.
- Getter form (``PRAGMA busy_timeout``) emits the current value
  without modifying it.
- Case-insensitive and whitespace-tolerant per SQLite parser
  conventions.
- Negative values clamp to 0 (stdlib parity — SQLite's
  ``sqlite3_busy_timeout`` clamps).
- Other PRAGMAs are NOT intercepted (regression — only busy_timeout).
- Multi-statement input does NOT match (routes to the wire where
  the existing classifier rejects it).
- ``_rowcount = -1`` matches stdlib's "no meaningful count for
  PRAGMA" convention.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import NUMBER
from dqlitedbapi._pragma_intercept import try_intercept_busy_timeout
from dqlitewire import ValueType


class _FakeCursor:
    """Minimal cursor stand-in for testing the interception. Real
    Cursor has more state; we only need _connection, _description,
    _rows, _rowcount, _row_index."""

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
    """``PRAGMA busy_timeout = N`` updates the connection's busy_timeout
    (N is ms; stored as seconds) and emits the new value as a row."""
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
    """The intercept emits an integer value, so its description
    ``type_code`` must be the wire-level INTEGER code (which compares
    equal to the ``NUMBER`` Type Object), matching the contract the
    normal wire path honours for every column. Emitting ``None`` would
    break ``cur.description[0][1] == NUMBER`` introspection that works
    everywhere else in the driver.
    """
    cur = _FakeCursor(busy_timeout_seconds=7.5)
    intercepted = try_intercept_busy_timeout(cur, "PRAGMA busy_timeout", None)
    assert intercepted is True
    assert cur._description is not None
    type_code = cur._description[0][1]
    assert type_code == int(ValueType.INTEGER)
    assert type_code == NUMBER


def test_setter_paren_form_updates_busy_timeout() -> None:
    """``PRAGMA busy_timeout(N)`` is an accepted SQLite alt form."""
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
    """``PRAGMA busy_timeout`` (no value) is the getter — emits the
    current value without modifying it."""
    cur = _FakeCursor(busy_timeout_seconds=7.5)
    intercepted = try_intercept_busy_timeout(
        cur,
        "PRAGMA busy_timeout",
        None,
    )
    assert intercepted is True
    assert cur._connection._busy_timeout == 7.5  # unchanged
    assert cur._rows == [(7500,)]


def test_case_insensitive() -> None:
    """SQLite's lexer is case-insensitive; intercept must match."""
    for variant in [
        "pragma busy_timeout = 5000",
        "PRAGMA BUSY_TIMEOUT = 5000",
        "Pragma Busy_Timeout = 5000",
    ]:
        cur = _FakeCursor()
        assert try_intercept_busy_timeout(cur, variant, None) is True, variant


def test_whitespace_tolerant() -> None:
    """SQLite tolerates extra whitespace around the ``=``."""
    for variant in [
        "PRAGMA  busy_timeout=5000",
        "  PRAGMA busy_timeout  =  5000  ",
        "PRAGMA\tbusy_timeout\t=\t5000",
        "PRAGMA busy_timeout = 5000;",
    ]:
        cur = _FakeCursor()
        assert try_intercept_busy_timeout(cur, variant, None) is True, variant


def test_negative_value_clamps_to_zero() -> None:
    """SQLite clamps negative busy_timeout to 0. Stdlib parity."""
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
    """``PRAGMA busy_timeout = 0`` is the canonical 'no retry' setter
    (stdlib parity for ``timeout=0``)."""
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
    """Regression: only busy_timeout is intercepted. Other PRAGMAs
    flow to the wire as before."""
    for statement in [
        "PRAGMA foreign_keys = ON",
        "PRAGMA journal_mode",
        "PRAGMA cache_size = 10000",
        "PRAGMA busy_timeoutXXX = 5000",  # name collision check
    ]:
        cur = _FakeCursor()
        assert try_intercept_busy_timeout(cur, statement, None) is False, statement


def test_multi_statement_not_intercepted() -> None:
    """``PRAGMA busy_timeout = N; SELECT 1`` has trailing content
    AFTER the optional trailing semicolon — does not match. Routes
    to the wire where the existing multi-statement classifier
    rejects it."""
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
    """Belt-and-suspenders: a non-str operation (caller bug) doesn't
    crash the interceptor — returns False so the upstream caller's
    own type check fires."""
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
    """Parametrised pin across the setter variants SQLite documents."""
    cur = _FakeCursor()
    intercepted = try_intercept_busy_timeout(cur, statement, None)
    assert intercepted is True
    assert cur._connection._busy_timeout == 5.0
