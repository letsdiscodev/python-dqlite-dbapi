"""Tests for exception classes."""

import pytest

from dqlitedbapi.exceptions import (
    DatabaseError,
    Error,
    InterfaceError,
    OperationalError,
    Warning,
)


class TestExceptionRaising:
    def test_raise_warning(self) -> None:
        with pytest.raises(Warning):
            raise Warning("test warning")

    def test_raise_error(self) -> None:
        with pytest.raises(Error):
            raise Error("test error")

    def test_raise_interface_error(self) -> None:
        with pytest.raises(InterfaceError):
            raise InterfaceError("interface error")

    def test_raise_database_error(self) -> None:
        with pytest.raises(DatabaseError):
            raise DatabaseError("database error")

    def test_error_is_exception_subclass(self) -> None:
        assert issubclass(Error, Exception)

    def test_catch_database_error_as_error(self) -> None:
        with pytest.raises(Error):
            raise DatabaseError("test")

    def test_catch_operational_error_as_database_error(self) -> None:
        with pytest.raises(DatabaseError):
            raise OperationalError("test")
