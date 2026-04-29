"""Pin the bare-dbapi ``autocommit`` property exposed on Connection
and AsyncConnection.

Stdlib ``sqlite3`` added ``Connection.autocommit`` in Python 3.12;
``psycopg`` exposes it as well. dqlite is genuinely autocommit-by-
default at the wire level (every statement commits unless the
caller issued an explicit BEGIN), so the bare dbapi reports
``True``. The SA adapter (sqlalchemy-dqlite) deliberately reports
``False`` because SA wraps the connection with explicit
BEGIN/COMMIT — both are accurate for their respective layer.

The setter accepts ``True`` as a no-op (mirroring the existing
mode) and rejects ``False`` with ``NotSupportedError`` because
the autocommit mode is fixed by the dqlite server.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import NotSupportedError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


class TestSyncAutocommitProperty:
    def test_autocommit_returns_true(self) -> None:
        conn = Connection.__new__(Connection)
        assert conn.autocommit is True

    def test_setting_true_is_noop(self) -> None:
        conn = Connection.__new__(Connection)
        conn.autocommit = True
        assert conn.autocommit is True

    def test_setting_false_raises_not_supported(self) -> None:
        conn = Connection.__new__(Connection)
        with pytest.raises(NotSupportedError, match="autocommit-by-default"):
            conn.autocommit = False


class TestAsyncAutocommitProperty:
    def test_autocommit_returns_true(self) -> None:
        conn = AsyncConnection.__new__(AsyncConnection)
        assert conn.autocommit is True

    def test_setting_true_is_noop(self) -> None:
        conn = AsyncConnection.__new__(AsyncConnection)
        conn.autocommit = True
        assert conn.autocommit is True

    def test_setting_false_raises_not_supported(self) -> None:
        conn = AsyncConnection.__new__(AsyncConnection)
        with pytest.raises(NotSupportedError, match="autocommit-by-default"):
            conn.autocommit = False
