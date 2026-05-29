"""Pin: every dbapi exception caps ``message`` (``args[0]``, used by
``str``/``repr``/pickle) at the same 4 KiB budget as ``raw_message``."""

from __future__ import annotations

import pickle

import pytest

from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
)

# 4 KiB cap + ~50-byte truncation suffix, plus headroom.
_CAP_BUDGET = 5000


@pytest.mark.parametrize(
    "cls",
    [
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
    ],
)
def test_message_arg_capped_at_4kb(cls: type) -> None:
    """A 63 KiB ``message`` produces a ``str(exc)`` bounded at the cap."""
    big = "X" * 63_000
    e = cls(big, code=42)
    rendered = str(e)
    assert len(rendered) < _CAP_BUDGET, (
        f"{cls.__name__} args[0] not capped: len(str(exc)) == {len(rendered)}"
    )


@pytest.mark.parametrize(
    "cls",
    [
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
    ],
)
def test_repr_capped_via_args0(cls: type) -> None:
    """``repr(exc)`` inherits the args[0] bound; 6 KiB absorbs escape inflation."""
    big = "X" * 63_000
    e = cls(big, code=42)
    rendered = repr(e)
    assert len(rendered) < 6000, (
        f"{cls.__name__} repr() not bounded: len(repr(exc)) == {len(rendered)}"
    )


def test_pickled_exception_bounded() -> None:
    """The pickled payload stays inside the budget once args[0] is capped."""
    big = "X" * 63_000
    e = OperationalError(big, code=1)
    pickled = pickle.dumps(e)
    assert len(pickled) < 16_000, (
        f"pickled OperationalError too large: {len(pickled)} bytes "
        f"(expected < 16 KiB after args[0] cap)"
    )


@pytest.mark.parametrize(
    "cls",
    [InterfaceError, DatabaseError, OperationalError],
)
def test_short_message_arg_round_trips(cls: type) -> None:
    """Short messages round-trip unchanged; the cap only fires past 4 KiB."""
    short = "ordinary error"
    e = cls(short, code=1)
    assert str(e) == short
