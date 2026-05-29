"""``_validate_ticks`` guards the ``*FromTicks`` constructors: it closes the
bool-through-int hole and rejects NaN/inf and non-numeric ticks as ``DataError``."""

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
            TimestampFromTicks(True)

    def test_timestamp_from_ticks_rejects_false(self) -> None:
        with pytest.raises(DataError, match="bool"):
            TimestampFromTicks(False)

    def test_date_from_ticks_rejects_bool(self) -> None:
        with pytest.raises(DataError, match="bool"):
            DateFromTicks(True)

    def test_time_from_ticks_rejects_bool(self) -> None:
        with pytest.raises(DataError, match="bool"):
            TimeFromTicks(True)


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
        result = TimestampFromTicks(Decimal("1700000000"))  # type: ignore[arg-type]
        assert result.year == 2023
