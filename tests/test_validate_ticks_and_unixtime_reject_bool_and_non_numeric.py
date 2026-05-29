"""``_validate_ticks`` and ``_datetime_from_unixtime`` reject ``bool`` and
non-numeric inputs guard-first, so almost-numeric types (e.g. ``numpy.bool_``)
cannot slip through and yield an epoch-based datetime."""

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
    """Reject a non-int/float even if its ``__float__`` yields a finite value."""

    class FakeBool:
        """Mimics numpy.bool_: not a bool subclass, but ``float()`` yields 1.0."""

        def __float__(self) -> float:
            return 1.0

    with pytest.raises(DataError):
        _validate_ticks(FakeBool())  # type: ignore[arg-type]


def test_validate_ticks_accepts_int_and_float() -> None:
    assert _validate_ticks(0) == 0.0
    assert _validate_ticks(1234567890) == 1234567890.0
    assert _validate_ticks(1.5) == 1.5
    assert _validate_ticks(-1.5) == -1.5


def test_validate_ticks_wraps_overflow_from_float_coercion() -> None:
    """An ``OverflowError`` from ``__float__`` surfaces as ``DataError``, not a
    bare ``OverflowError``. Defensive: today CPython saturates to ``inf``."""
    from decimal import Decimal

    class RaisingDecimal(Decimal):
        """Decimal lineage passes the isinstance guard; ``__float__`` overflows."""

        def __float__(self) -> float:
            raise OverflowError("custom overflow from __float__")

    with pytest.raises(DataError, match="overflow"):
        _validate_ticks(RaisingDecimal("1"))  # type: ignore[arg-type]


def test_datetime_from_unixtime_rejects_bool() -> None:
    """Bool slipped past the range check and produced epoch-based datetimes."""
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
    dt = _datetime_from_unixtime(0)
    assert dt.year == 1970
    dt = _datetime_from_unixtime(1700000000)
    assert dt.year == 2023
