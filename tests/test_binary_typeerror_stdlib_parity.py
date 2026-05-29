"""Pin: ``Binary`` is the stdlib memoryview alias and deliberately leaks bare
TypeError (outside the Error hierarchy) on bad input, for stdlib drop-in parity.
Sibling Date/Time/Timestamp constructors wrap as DataError; Binary does not."""

from __future__ import annotations

import pytest

import dqlitedbapi


def test_binary_is_memoryview_alias() -> None:
    """Pin the stdlib-parity alias so it cannot drift to a wrapper."""
    assert dqlitedbapi.Binary is memoryview


def test_binary_round_trips_bytes() -> None:
    mv = dqlitedbapi.Binary(b"hello")
    assert isinstance(mv, memoryview)
    assert bytes(mv) == b"hello"


def test_binary_bad_input_raises_bare_typeerror_not_dbapi_error() -> None:
    """Deliberate: bare TypeError preserves stdlib drop-in parity."""
    with pytest.raises(TypeError):
        dqlitedbapi.Binary("not bytes")  # type: ignore[arg-type]


def test_binary_int_raises_bare_typeerror() -> None:
    with pytest.raises(TypeError):
        dqlitedbapi.Binary(123)  # type: ignore[arg-type]


def test_binary_none_raises_bare_typeerror() -> None:
    with pytest.raises(TypeError):
        dqlitedbapi.Binary(None)  # type: ignore[arg-type]


def test_binary_typeerror_is_not_caught_by_dbapi_error_hierarchy() -> None:
    """Documented escape: porting callers must wrap their own try/except."""
    with pytest.raises(TypeError):
        try:
            dqlitedbapi.Binary("not bytes")  # type: ignore[arg-type]
        except dqlitedbapi.Error:
            pytest.fail("Binary unexpectedly inside dbapi.Error hierarchy")
