"""Unit tests for ``_validate_ticks`` — the guard PEP 249's three
``*FromTicks`` constructors run before handing ``ticks`` to
``datetime.fromtimestamp``.

Closes the bool-through-int hole (``bool`` is an ``int`` subclass,
so ``isinstance(True, float)`` is False and the old guard let
``TimestampFromTicks(True)`` silently return ``datetime(1970-01-01
00:00:01)``). Also rejects ``Decimal("NaN")`` and unsupported
non-numeric types up front so every failure mode surfaces as a
single DB-API ``DataError``.
"""

from __future__ import annotations

import math
from decimal import Decimal

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import (
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
)


class TestValidateTicksRejectsBool:
    def test_timestamp_from_ticks_rejects_true(self) -> None:
        with pytest.raises(DataError, match="bool"):
            TimestampFromTicks(True)  # type: ignore[arg-type]

    def test_timestamp_from_ticks_rejects_false(self) -> None:
        with pytest.raises(DataError, match="bool"):
            TimestampFromTicks(False)  # type: ignore[arg-type]

    def test_date_from_ticks_rejects_bool(self) -> None:
        with pytest.raises(DataError, match="bool"):
            DateFromTicks(True)  # type: ignore[arg-type]

    def test_time_from_ticks_rejects_bool(self) -> None:
        with pytest.raises(DataError, match="bool"):
            TimeFromTicks(True)  # type: ignore[arg-type]


class TestValidateTicksRejectsDecimalNan:
    def test_timestamp_from_ticks_rejects_decimal_nan(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(Decimal("NaN"))  # type: ignore[arg-type]

    def test_timestamp_from_ticks_rejects_decimal_inf(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(Decimal("Infinity"))  # type: ignore[arg-type]


class TestValidateTicksRejectsFloatNanInf:
    def test_rejects_nan(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(math.nan)

    def test_rejects_inf(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(math.inf)


class TestValidateTicksRejectsNonNumeric:
    def test_rejects_str(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks("1700000000")  # type: ignore[arg-type]

    def test_rejects_none(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(None)  # type: ignore[arg-type]


class TestValidateTicksHappyPath:
    def test_int_accepted(self) -> None:
        assert TimestampFromTicks(1700000000).year == 2023

    def test_float_accepted_preserving_fraction(self) -> None:
        assert TimestampFromTicks(1700000000.5).microsecond == 500_000

    def test_decimal_finite_accepted(self) -> None:
        # A finite Decimal passes the guard via ``float(ticks)`` coercion.
        result = TimestampFromTicks(Decimal("1700000000"))  # type: ignore[arg-type]
        assert result.year == 2023
