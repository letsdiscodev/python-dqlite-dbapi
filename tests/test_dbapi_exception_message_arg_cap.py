"""Defense-in-depth pin: every dbapi exception class caps the
``message`` argument (i.e. ``args[0]``, the value used by
``str(exc)``, ``repr(exc)``, and pickling) at the same 4 KiB budget
the ``raw_message`` cap uses.

Without the cap, the wire-layer 64 KiB ``FailureResponse`` ceiling
flows through to ``args[0]`` and amplifies across:

- Celery / multiprocessing pickled exception payloads (per-attempt
  retry result pickle).
- BaseExceptionGroup fan-out aggregating many failures.
- ``logging.exception`` traceback formatters that materialise
  ``repr(exc)`` at full size — the repr's quoting overhead on
  control-byte-heavy peer text can EXCEED the ``args[0]`` byte
  size (Amplification 2-4× from Python's repr escape rules).

The companion ``raw_message`` cap already exists
(``test_dbapi_exception_raw_message_cap``); this file mirrors the
discipline at the ``message`` arg + repr surface.
"""

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

# 4 KiB cap + ~50-byte truncation suffix; allow some headroom for
# the suffix format string in the wire helper.
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
    """A 63 KiB ``message`` argument (well below the wire cap)
    produces a ``str(exc)`` bounded at the 4 KiB cap + truncation
    suffix — not the full 63 KiB."""
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
    """``repr(exc)`` derives from ``args[0]``; once args[0] is
    bounded the repr inherits the bound (modulo Python's quoting
    overhead and class-name / code field). 6 KiB ceiling absorbs
    the worst-case repr escape inflation on a 4 KiB capped string."""
    big = "X" * 63_000
    e = cls(big, code=42)
    rendered = repr(e)
    assert len(rendered) < 6000, (
        f"{cls.__name__} repr() not bounded: len(repr(exc)) == {len(rendered)}"
    )


def test_pickled_exception_bounded() -> None:
    """The pickled payload — what Celery / multiprocessing
    serialises — stays inside the same budget. Previously the
    uncapped ``args[0]`` blew the pickle out to 64 KiB+ per
    exception."""
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
    """Negative pin: short messages round-trip unchanged through
    args[0] — the cap only kicks in past the 4 KiB threshold."""
    short = "ordinary error"
    e = cls(short, code=1)
    assert str(e) == short
