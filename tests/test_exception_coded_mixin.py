"""Pin the private ``_DatabaseErrorWithCode`` base shared by the three coded-error classes.

It is an implementation detail (not public PEP 249 surface): a refactor must not silently
move classes into/out of the coded subfamily, break the hierarchy, or move ``code`` into
``args`` (which would lose ``.code`` on pickle round-trip).
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
    """Coded-error classes inherit ``_DatabaseErrorWithCode``; other subclasses do not."""

    @pytest.mark.parametrize(
        "cls",
        [OperationalError, IntegrityError, InternalError, DataError, ProgrammingError],
    )
    def test_coded_class_is_in_family(self, cls: type[_DatabaseErrorWithCode]) -> None:
        assert issubclass(cls, _DatabaseErrorWithCode)
        inst = cls("x", code=5)
        assert isinstance(inst, _DatabaseErrorWithCode)

    @pytest.mark.parametrize(
        "cls",
        [NotSupportedError, InterfaceError],
    )
    def test_uncoded_class_is_not_in_family(self, cls: type[Exception]) -> None:
        assert not issubclass(cls, _DatabaseErrorWithCode)
        inst = cls("x")
        assert not isinstance(inst, _DatabaseErrorWithCode)


class TestPEP249HierarchyPreserved:
    """The intermediate base must not break the PEP 249 ``Error -> DatabaseError ->
    <concrete>`` chain that users catch on."""

    @pytest.mark.parametrize("cls", [OperationalError, IntegrityError, InternalError])
    def test_coded_class_is_database_error(self, cls: type[Exception]) -> None:
        assert issubclass(cls, DatabaseError)
        assert issubclass(cls, Error)
        assert issubclass(cls, Exception)

    def test_mixin_itself_is_database_error(self) -> None:
        assert issubclass(_DatabaseErrorWithCode, DatabaseError)
        assert issubclass(_DatabaseErrorWithCode, Error)


class TestNotPubliclyExported:
    """``_DatabaseErrorWithCode`` is private: not in ``__all__``."""

    def test_private_class_not_in_module_all(self) -> None:
        assert "_DatabaseErrorWithCode" not in _exceptions_module.__all__


class TestPickleRoundTrip:
    """Default ``__reduce_ex__`` preserves ``.code`` because ``args == (message,)``."""

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
