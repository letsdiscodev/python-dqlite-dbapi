"""Tests for async module PEP 249 attributes and exports."""

from dqlitedbapi import aio


class TestAioModuleAttributes:
    def test_apilevel(self) -> None:
        assert aio.apilevel == "2.0"

    def test_threadsafety(self) -> None:
        assert aio.threadsafety == 1

    def test_paramstyle(self) -> None:
        assert aio.paramstyle == "qmark"

    def test_type_constructors_exported(self) -> None:
        """PEP 249 type constructors should be available from aio module."""
        assert callable(aio.Date)
        assert callable(aio.Time)
        assert callable(aio.Timestamp)
        assert callable(aio.DateFromTicks)
        assert callable(aio.TimeFromTicks)
        assert callable(aio.TimestampFromTicks)
        assert callable(aio.Binary)

    def test_type_objects_exported(self) -> None:
        """PEP 249 type objects should be available from aio module."""
        assert aio.STRING == "TEXT"
        assert aio.BINARY == "BLOB"
        assert aio.NUMBER == "INTEGER"
        assert aio.DATETIME == "DATE"
        assert aio.ROWID == "ROWID"
