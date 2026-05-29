"""Coded-error classes surface ``.code`` via ``repr``.

The default ``repr`` drops attributes not in ``args``, hiding ``.code`` from
``repr(exc)`` consumers (Sentry, ``logger.error("%r", exc)``); ``__repr__`` is overridden.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import IntegrityError, InternalError, OperationalError


def test_operational_error_repr_includes_code() -> None:
    exc = OperationalError("busy", code=5)
    assert repr(exc) == "OperationalError('busy', code=5)"


def test_operational_error_repr_without_code() -> None:
    exc = OperationalError("plain")
    assert repr(exc) == "OperationalError('plain')"


def test_integrity_error_repr_includes_code() -> None:
    exc = IntegrityError("UNIQUE", code=2067)
    assert repr(exc) == "IntegrityError('UNIQUE', code=2067)"


def test_integrity_error_repr_without_code() -> None:
    exc = IntegrityError("constraint")
    assert repr(exc) == "IntegrityError('constraint')"


def test_internal_error_repr_includes_code() -> None:
    exc = InternalError("sqlite internal", code=2)
    assert repr(exc) == "InternalError('sqlite internal', code=2)"


def test_internal_error_repr_without_code() -> None:
    exc = InternalError("plain")
    assert repr(exc) == "InternalError('plain')"


def test_str_unchanged() -> None:
    """str(exc) still returns only the message, not the code."""
    assert str(OperationalError("plain", code=5)) == "plain"
    assert str(IntegrityError("x", code=2067)) == "x"
    assert str(InternalError("y", code=2)) == "y"
