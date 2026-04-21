"""Tests for PEP 249 module-level attributes."""

import dqlitedbapi


class TestModuleAttributes:
    def test_apilevel(self) -> None:
        assert dqlitedbapi.apilevel == "2.0"

    def test_threadsafety(self) -> None:
        # 1 = threads may share the module, but not connections
        assert dqlitedbapi.threadsafety == 1

    def test_paramstyle(self) -> None:
        assert dqlitedbapi.paramstyle == "qmark"

    def test_connect_function_exists(self) -> None:
        assert callable(dqlitedbapi.connect)


class TestExceptions:
    def test_exception_hierarchy(self) -> None:
        # All exceptions should derive from Error except Warning
        assert issubclass(dqlitedbapi.Error, Exception)
        assert issubclass(dqlitedbapi.Warning, Exception)
        assert issubclass(dqlitedbapi.InterfaceError, dqlitedbapi.Error)
        assert issubclass(dqlitedbapi.DatabaseError, dqlitedbapi.Error)
        assert issubclass(dqlitedbapi.DataError, dqlitedbapi.DatabaseError)
        assert issubclass(dqlitedbapi.OperationalError, dqlitedbapi.DatabaseError)
        assert issubclass(dqlitedbapi.IntegrityError, dqlitedbapi.DatabaseError)
        assert issubclass(dqlitedbapi.InternalError, dqlitedbapi.DatabaseError)
        assert issubclass(dqlitedbapi.ProgrammingError, dqlitedbapi.DatabaseError)
        assert issubclass(dqlitedbapi.NotSupportedError, dqlitedbapi.DatabaseError)


class TestTypeConstructors:
    def test_date(self) -> None:
        import datetime

        d = dqlitedbapi.Date(2024, 1, 15)
        assert isinstance(d, datetime.date)
        assert d.year == 2024
        assert d.month == 1
        assert d.day == 15

    def test_time(self) -> None:
        import datetime

        t = dqlitedbapi.Time(10, 30, 45)
        assert isinstance(t, datetime.time)
        assert t.hour == 10
        assert t.minute == 30
        assert t.second == 45

    def test_timestamp(self) -> None:
        import datetime

        ts = dqlitedbapi.Timestamp(2024, 1, 15, 10, 30, 45)
        assert isinstance(ts, datetime.datetime)
        assert ts.year == 2024
        assert ts.hour == 10

    def test_binary(self) -> None:
        b = dqlitedbapi.Binary(b"hello")
        assert isinstance(b, bytes)
        assert b == b"hello"


class TestTypeObjects:
    def test_string_type(self) -> None:
        assert dqlitedbapi.STRING == "TEXT"
        assert dqlitedbapi.STRING == "VARCHAR"
        assert dqlitedbapi.STRING == "text"
        assert dqlitedbapi.STRING != "INTEGER"

    def test_binary_type(self) -> None:
        assert dqlitedbapi.BINARY == "BLOB"
        assert dqlitedbapi.BINARY != "TEXT"

    def test_number_type(self) -> None:
        assert dqlitedbapi.NUMBER == "INTEGER"
        assert dqlitedbapi.NUMBER == "REAL"
        assert dqlitedbapi.NUMBER == "FLOAT"
        assert dqlitedbapi.NUMBER != "TEXT"

    def test_datetime_type(self) -> None:
        assert dqlitedbapi.DATETIME == "DATETIME"
        assert dqlitedbapi.DATETIME == "DATE"
        assert dqlitedbapi.DATETIME == "TIMESTAMP"

    def test_rowid_type(self) -> None:
        assert dqlitedbapi.ROWID == "ROWID"


class TestDBAPITypeRepr:
    def test_named_types_have_readable_repr(self) -> None:
        assert repr(dqlitedbapi.STRING) == "STRING"
        assert repr(dqlitedbapi.BINARY) == "BINARY"
        assert repr(dqlitedbapi.NUMBER) == "NUMBER"
        assert repr(dqlitedbapi.DATETIME) == "DATETIME"
        assert repr(dqlitedbapi.ROWID) == "ROWID"


class TestCursorModuleAll:
    def test_cursor_module_has_all(self) -> None:
        from dqlitedbapi import cursor as cursor_mod

        assert cursor_mod.__all__ == ["Cursor"]

    def test_cursor_module_wildcard_import_does_not_leak_helpers(self) -> None:
        from dqlitedbapi import cursor as cursor_mod

        # Verify private helpers exist on the module but are not in __all__.
        assert hasattr(cursor_mod, "_call_client")
        assert "_call_client" not in cursor_mod.__all__


class TestExceptionsModuleAll:
    """``dqlitedbapi.exceptions`` re-exports the full PEP 249 class
    hierarchy. Pin the ``__all__`` list so a future refactor adding a
    private helper to this module does not leak through
    ``from dqlitedbapi.exceptions import *``.
    """

    def test_exceptions_module_has_all(self) -> None:
        from dqlitedbapi import exceptions as exc_mod

        assert sorted(exc_mod.__all__) == sorted(
            [
                "Warning",
                "Error",
                "InterfaceError",
                "DatabaseError",
                "DataError",
                "OperationalError",
                "IntegrityError",
                "InternalError",
                "ProgrammingError",
                "NotSupportedError",
            ]
        )

    def test_exceptions_module_all_entries_resolve(self) -> None:
        from dqlitedbapi import exceptions as exc_mod

        for name in exc_mod.__all__:
            assert hasattr(exc_mod, name), f"__all__ lists {name!r} but module lacks it"
            assert issubclass(getattr(exc_mod, name), Exception)


class TestTypesModuleAll:
    """``dqlitedbapi.types`` re-exports PEP 249 type constructors and
    type objects. Pin the ``__all__`` list so private helpers
    (``_iso8601_from_datetime``, ``_DescriptionTuple``, ...) stay
    private.
    """

    def test_types_module_has_all(self) -> None:
        from dqlitedbapi import types as types_mod

        assert sorted(types_mod.__all__) == sorted(
            [
                "Date",
                "Time",
                "Timestamp",
                "DateFromTicks",
                "TimeFromTicks",
                "TimestampFromTicks",
                "Binary",
                "STRING",
                "BINARY",
                "NUMBER",
                "DATETIME",
                "ROWID",
            ]
        )

    def test_types_module_all_entries_resolve(self) -> None:
        from dqlitedbapi import types as types_mod

        for name in types_mod.__all__:
            assert hasattr(types_mod, name), f"__all__ lists {name!r} but module lacks it"

    def test_types_module_wildcard_import_does_not_leak_private_helpers(self) -> None:
        from dqlitedbapi import types as types_mod

        # Sanity: the private alias lives on the module but stays out of __all__.
        assert hasattr(types_mod, "_Description")
        assert "_Description" not in types_mod.__all__
        assert "_DescriptionTuple" not in types_mod.__all__
        assert "_iso8601_from_datetime" not in types_mod.__all__
