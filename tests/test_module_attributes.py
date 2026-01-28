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
