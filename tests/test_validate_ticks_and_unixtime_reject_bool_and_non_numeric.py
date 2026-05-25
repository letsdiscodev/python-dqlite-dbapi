"""Pin: ``_validate_ticks`` and ``_datetime_from_unixtime`` reject
``bool`` and non-numeric inputs uniformly, mirroring the wire-layer
``encode_double`` discipline.

Before the fix:
- ``_validate_ticks`` rejected Python ``bool`` and ``str`` but
  silently accepted ``numpy.bool_`` (not a Python bool subclass) and
  coerced it to ``1.0`` / ``0.0`` — yielding an epoch-based date.
- ``_datetime_from_unixtime`` had a predicate that short-circuited
  on ``bool``: a ``True`` / ``False`` value slipped past the range
  check and fell into ``fromtimestamp`` as epoch + 0 / + 1 second.

Both now apply guard-first ordering: reject bool, reject non-numeric,
then range-check, then coerce. ``numpy.bool_`` and other almost-
numeric types are uniformly rejected.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _datetime_from_unixtime, _validate_ticks


def test_validate_ticks_rejects_python_bool() -> None:
    with pytest.raises(DataError, match="bool"):
        _validate_ticks(True)
    with pytest.raises(DataError, match="bool"):
        _validate_ticks(False)


def test_validate_ticks_rejects_non_numeric() -> None:
    """A non-int / non-float input must be rejected even if it has
    a __float__ method that would produce a finite value. This is
    the numpy.bool_ class of bug."""

    class FakeBool:
        """Mimics numpy.bool_: not a Python bool subclass, but
        ``float(FakeBool())`` succeeds and yields 1.0."""

        def __float__(self) -> float:
            return 1.0

    with pytest.raises(DataError):
        _validate_ticks(FakeBool())  # type: ignore[arg-type]


def test_validate_ticks_accepts_int_and_float() -> None:
    """Sibling positive: valid numeric inputs round-trip."""
    assert _validate_ticks(0) == 0.0
    assert _validate_ticks(1234567890) == 1234567890.0
    assert _validate_ticks(1.5) == 1.5
    assert _validate_ticks(-1.5) == -1.5


def test_validate_ticks_wraps_overflow_from_float_coercion() -> None:
    """A numeric subclass whose ``__float__`` raises ``OverflowError``
    must surface as ``DataError`` — not a bare ``OverflowError`` that
    escapes the ``dbapi.Error`` hierarchy.

    Today's CPython ``float(Decimal('1e1000000'))`` saturates to
    ``inf`` (caught by the ``math.isfinite`` arm), so the path is not
    currently exposed in the wild. The catch is defensive against a
    future CPython release that flips the saturation to a raise, and
    against custom numeric subclasses whose ``__float__`` propagates
    an ``OverflowError`` from a precision-context trap.
    """
    from decimal import Decimal

    class RaisingDecimal(Decimal):
        """Mimic a Decimal whose ``__float__`` propagates an overflow
        rather than saturating to ``inf``. The ``Decimal`` lineage
        passes the isinstance guard at the top of ``_validate_ticks``;
        the ``__float__`` override exercises the catch on the inner
        coercion."""

        def __float__(self) -> float:
            raise OverflowError("custom overflow from __float__")

    with pytest.raises(DataError, match="overflow"):
        _validate_ticks(RaisingDecimal("1"))  # type: ignore[arg-type]


def test_datetime_from_unixtime_rejects_bool() -> None:
    """Bool slipped past the range-check predicate and silently
    produced epoch-based datetimes. The guard-first ordering rejects
    explicitly."""
    with pytest.raises(DataError, match="bool"):
        _datetime_from_unixtime(True)
    with pytest.raises(DataError, match="bool"):
        _datetime_from_unixtime(False)


def test_datetime_from_unixtime_rejects_non_int() -> None:
    with pytest.raises(DataError, match="must be int"):
        _datetime_from_unixtime(1.5)  # type: ignore[arg-type]
    with pytest.raises(DataError, match="must be int"):
        _datetime_from_unixtime("1234")  # type: ignore[arg-type]


def test_datetime_from_unixtime_accepts_int_in_range() -> None:
    """Sibling positive: valid int input round-trips to UTC datetime."""
    dt = _datetime_from_unixtime(0)
    assert dt.year == 1970
    dt = _datetime_from_unixtime(1700000000)
    assert dt.year == 2023
