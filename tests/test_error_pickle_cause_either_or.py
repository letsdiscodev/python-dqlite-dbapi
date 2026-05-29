"""Pin: dbapi ``Error.__cause__`` either round-trips through pickle or is dropped; never partial."""

from __future__ import annotations

import copy
import pickle

from dqlitedbapi.exceptions import (
    DatabaseError,
    InterfaceError,
    OperationalError,
)


def test_database_error_cause_pickle_either_or() -> None:
    inner = ValueError("inner forensic state")
    outer = DatabaseError("outer wrap")
    outer.__cause__ = inner
    restored = pickle.loads(pickle.dumps(outer))
    if restored.__cause__ is not None:
        assert isinstance(restored.__cause__, ValueError)


def test_interface_error_cause_deepcopy_either_or() -> None:
    """deepcopy goes through __reduce__ same as pickle."""
    inner = RuntimeError("inner")
    outer = InterfaceError("outer")
    outer.__cause__ = inner
    restored = copy.deepcopy(outer)
    if restored.__cause__ is not None:
        assert isinstance(restored.__cause__, RuntimeError)


def test_operational_error_cause_pickle_either_or() -> None:
    inner = ValueError("wire-level cause")
    outer = OperationalError("dbapi msg", 1)
    outer.__cause__ = inner
    restored = pickle.loads(pickle.dumps(outer))
    if restored.__cause__ is not None:
        assert isinstance(restored.__cause__, ValueError)
    assert "dbapi msg" in str(restored)
