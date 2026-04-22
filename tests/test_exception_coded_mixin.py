"""The three coded-error classes share a private ``_DatabaseErrorWithCode``
base.

This intermediate base holds the ``__init__(message, code=...)`` and
``__repr__`` that surface the SQLite extended error code. It is a
deliberate implementation detail — **not** part of the public PEP 249
hierarchy — so these tests import it via its private name to pin the
contract that a future refactor cannot silently promote/demote classes
into or out of the coded-subfamily.

Also pins:
- PEP 249 hierarchy preservation (OperationalError is still a
  DatabaseError is still an Error is still an Exception — the
  intermediate class sits between DatabaseError and the concrete).
- Pickle round-trip preservation of ``.code`` — guards against a
  future refactor that moves ``code`` into ``args`` (which would
  break every caller that stores these exceptions in
  Sentry-style queues).
- The mixin is **not** re-exported via ``__all__``.
"""

from __future__ import annotations

import pickle

import pytest

import dqlitedbapi.exceptions as _exceptions_module
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    _DatabaseErrorWithCode,
)


class TestCodedFamilyMembership:
    """The three coded-error classes inherit from ``_DatabaseErrorWithCode``;
    the other PEP 249 DatabaseError subclasses do not."""

    @pytest.mark.parametrize("cls", [OperationalError, IntegrityError, InternalError])
    def test_coded_class_is_in_family(self, cls: type[_DatabaseErrorWithCode]) -> None:
        assert issubclass(cls, _DatabaseErrorWithCode)
        inst = cls("x", code=5)
        assert isinstance(inst, _DatabaseErrorWithCode)

    @pytest.mark.parametrize(
        "cls",
        [DataError, ProgrammingError, NotSupportedError, InterfaceError],
    )
    def test_uncoded_class_is_not_in_family(self, cls: type[Exception]) -> None:
        assert not issubclass(cls, _DatabaseErrorWithCode)
        inst = cls("x")
        assert not isinstance(inst, _DatabaseErrorWithCode)


class TestPEP249HierarchyPreserved:
    """Inserting ``_DatabaseErrorWithCode`` between ``DatabaseError`` and
    the three concrete classes must not break the PEP 249 ``Error ->
    DatabaseError -> <concrete>`` chain that users catch on.
    """

    @pytest.mark.parametrize("cls", [OperationalError, IntegrityError, InternalError])
    def test_coded_class_is_database_error(self, cls: type[Exception]) -> None:
        assert issubclass(cls, DatabaseError)
        assert issubclass(cls, Error)
        assert issubclass(cls, Exception)

    def test_mixin_itself_is_database_error(self) -> None:
        assert issubclass(_DatabaseErrorWithCode, DatabaseError)
        assert issubclass(_DatabaseErrorWithCode, Error)


class TestNotPubliclyExported:
    """``_DatabaseErrorWithCode`` is private — not in ``__all__``, and not
    part of the PEP 249 surface."""

    def test_private_class_not_in_module_all(self) -> None:
        assert "_DatabaseErrorWithCode" not in _exceptions_module.__all__


class TestPickleRoundTrip:
    """Default ``__reduce_ex__`` preserves ``.code`` because ``__init__``
    takes ``(message, code=...)`` and ``args == (message,)``. Pinning
    this so a future refactor that moves ``code`` into ``args`` is a
    visible contract change rather than silent breakage.
    """

    @pytest.mark.parametrize("cls", [OperationalError, IntegrityError, InternalError])
    def test_pickle_preserves_code(self, cls: type[_DatabaseErrorWithCode]) -> None:
        original = cls("boom", code=2067)
        restored = pickle.loads(pickle.dumps(original))
        assert type(restored) is cls
        assert str(restored) == "boom"
        assert restored.code == 2067

    @pytest.mark.parametrize("cls", [OperationalError, IntegrityError, InternalError])
    def test_pickle_preserves_none_code(self, cls: type[_DatabaseErrorWithCode]) -> None:
        original = cls("plain")
        restored = pickle.loads(pickle.dumps(original))
        assert type(restored) is cls
        assert str(restored) == "plain"
        assert restored.code is None
