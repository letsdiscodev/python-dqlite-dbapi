"""Pin: ``Connection`` and ``AsyncConnection`` reject non-str
``database`` kwarg with ``InterfaceError`` at the construction site,
mirroring the existing ``address`` discipline.

Without this pin, a caller passing ``database=b"foo"`` flowed bytes
through to the wire layer where the encoder eventually produced a
less-actionable DataError far from the ergonomic site.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_connection_rejects_bytes_database() -> None:
    with pytest.raises(dqlitedbapi.InterfaceError, match="database must be a str"):
        dqlitedbapi.Connection("localhost:9001", database=b"foo")  # type: ignore[arg-type]


def test_sync_connection_rejects_none_database() -> None:
    with pytest.raises(dqlitedbapi.InterfaceError, match="database must be a str"):
        dqlitedbapi.Connection("localhost:9001", database=None)  # type: ignore[arg-type]


def test_sync_connection_accepts_str_database() -> None:
    conn = dqlitedbapi.Connection("localhost:9001", database="mydb")
    assert conn._database == "mydb"


def test_async_connection_rejects_bytes_database() -> None:
    with pytest.raises(dqlitedbapi.InterfaceError, match="database must be a str"):
        AsyncConnection("localhost:9001", database=b"foo")  # type: ignore[arg-type]


def test_async_connection_rejects_none_database() -> None:
    with pytest.raises(dqlitedbapi.InterfaceError, match="database must be a str"):
        AsyncConnection("localhost:9001", database=None)  # type: ignore[arg-type]


def test_async_connection_accepts_str_database() -> None:
    conn = AsyncConnection("localhost:9001", database="mydb")
    assert conn._database == "mydb"
