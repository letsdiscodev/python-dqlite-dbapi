"""PEP 249 ``Connection.Error`` alias coverage.

PEP 249 "Optional DB-API Extensions" expects every Connection to expose
the module-level exception classes as attributes so cross-driver
generic code (testing adapters, pool middleware, SQLAlchemy plugins)
can write ``except conn.Error:`` without importing the driver module.
Stdlib ``sqlite3.Connection`` and every mainstream DB-API driver ship
these; this test pins that dqlitedbapi matches.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi import Connection
from dqlitedbapi.aio import AsyncConnection

_PEP249_ALIAS_NAMES = (
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
)


@pytest.mark.parametrize("name", _PEP249_ALIAS_NAMES)
def test_sync_connection_alias_identity(name: str) -> None:
    alias = getattr(Connection, name)
    module_class = getattr(dqlitedbapi, name)
    assert alias is module_class, (
        f"Connection.{name} should be the same class object as dqlitedbapi.{name}"
    )


@pytest.mark.parametrize("name", _PEP249_ALIAS_NAMES)
def test_async_connection_alias_identity(name: str) -> None:
    alias = getattr(AsyncConnection, name)
    module_class = getattr(dqlitedbapi.aio, name)
    assert alias is module_class, (
        f"AsyncConnection.{name} should be the same class object as dqlitedbapi.aio.{name}"
    )


def test_operational_error_via_connection_alias_preserves_code() -> None:
    """The alias must be the SAME class, not a subclass, so custom
    ``__init__(message, code=...)`` continues to work.
    """
    with pytest.raises(Connection.OperationalError) as excinfo:
        raise Connection.OperationalError("explode", code=42)
    assert excinfo.value.code == 42
    assert str(excinfo.value) == "explode"


def test_catch_via_instance_attribute() -> None:
    """PEP 249's wording covers both ``Class.Error`` and ``instance.Error``."""
    conn = Connection("localhost:9001")
    try:
        raise dqlitedbapi.DataError("boom")
    except conn.Error as exc:
        assert isinstance(exc, dqlitedbapi.Error)


def test_async_catch_via_instance_attribute() -> None:
    aconn = AsyncConnection("localhost:9001")
    try:
        raise dqlitedbapi.DataError("boom")
    except aconn.Error as exc:
        assert isinstance(exc, dqlitedbapi.Error)
