"""Pin: ``Connection`` and ``AsyncConnection`` reject ``database=""``
(empty string) with ``InterfaceError`` at the construction site,
mirroring the address discipline (``_client_parse_address`` rejects
empty addresses).

Without this pin, an empty ``database`` flowed through to the wire
``OpenRequest("")`` with undefined server-side semantics. The dbapi
layer is the strict PEP 249 boundary; surfacing this at construction
beats a downstream wire-time failure.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_connection_rejects_empty_database() -> None:
    with pytest.raises(dqlitedbapi.InterfaceError, match="database must be a non-empty string"):
        dqlitedbapi.Connection("localhost:9001", database="")


def test_async_connection_rejects_empty_database() -> None:
    with pytest.raises(dqlitedbapi.InterfaceError, match="database must be a non-empty string"):
        AsyncConnection("localhost:9001", database="")


def test_sync_connection_accepts_non_empty_database() -> None:
    conn = dqlitedbapi.Connection("localhost:9001", database="default")
    assert conn._database == "default"


def test_async_connection_accepts_non_empty_database() -> None:
    conn = AsyncConnection("localhost:9001", database="default")
    assert conn._database == "default"
