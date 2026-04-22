"""dbapi coded-error classes must surface ``.code`` via repr.

Covers ``OperationalError``, ``IntegrityError``, and ``InternalError``
— the three dbapi classes that accept an optional SQLite extended
error ``code`` kwarg.

Sentry, Rollbar, and ``logger.error("%r", exc)`` call ``repr(exc)``,
which drops any attribute not in ``args``. The ``code`` kwarg is
stored on ``self.code`` so the default repr hid the SQLite extended
error code. Override ``__repr__`` so the code is visible without
reaching into ``.code`` manually.
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
    """str(exc) continues to return only the message, preserving
    downstream assertions that match on the string form.
    """
    assert str(OperationalError("plain", code=5)) == "plain"
    assert str(IntegrityError("x", code=2067)) == "x"
    assert str(InternalError("y", code=2)) == "y"
