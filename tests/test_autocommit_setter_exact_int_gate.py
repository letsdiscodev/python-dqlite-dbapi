"""Pin: ``Connection.autocommit`` setter uses an exact-int gate for
the ``-1`` (LEGACY_TRANSACTION_CONTROL) sentinel; loose ``value == -1``
equality used to accept ``-1.0``, ``Decimal('-1')`` and custom
``__eq__`` objects, breaking the cross-driver
``isinstance(conn.autocommit, int)`` introspection idiom.

Stdlib reference: CPython
``Modules/_sqlite/connection.c::pysqlite_connection_autocommit_setter``
uses an exact-int gate; ``conn.autocommit = -1.0`` raises ValueError.
The dbapi tightens to match.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import NotSupportedError


def _bare_sync_connection() -> Connection:
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = MagicMock()
    conn._check_thread = lambda: None
    conn.messages = []
    return conn


def _bare_async_connection() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._check_loop_binding = lambda: None
    conn.messages = []
    return conn


def test_sync_autocommit_setter_accepts_true_and_canonical_minus_one() -> None:
    conn = _bare_sync_connection()
    conn.autocommit = True
    assert conn.autocommit is True
    conn.autocommit = -1
    assert conn.autocommit == -1
    # Round-trip yields canonical int(-1).
    assert isinstance(conn.autocommit, int)
    assert type(conn.autocommit) is int


def test_sync_autocommit_setter_rejects_float_minus_one() -> None:
    conn = _bare_sync_connection()
    with pytest.raises(NotSupportedError):
        conn.autocommit = -1.0


def test_sync_autocommit_setter_rejects_decimal_minus_one() -> None:
    conn = _bare_sync_connection()
    with pytest.raises(NotSupportedError):
        conn.autocommit = Decimal("-1")


def test_sync_autocommit_setter_rejects_custom_eq_minus_one() -> None:
    class FakeNegOne:
        def __eq__(self, other: object) -> bool:
            return other == -1

        def __hash__(self) -> int:
            return -1

    conn = _bare_sync_connection()
    with pytest.raises(NotSupportedError):
        conn.autocommit = FakeNegOne()


def test_sync_autocommit_setter_canonicalises_int_subclass() -> None:
    """An ``IntEnum`` / int subclass equal to -1 is accepted via the
    isinstance(int) gate, but the stored slot is canonical
    ``int(-1)`` so the getter round-trips a plain int.
    """

    class IntSub(int):
        pass

    conn = _bare_sync_connection()
    conn.autocommit = IntSub(-1)
    assert conn.autocommit == -1
    assert type(conn.autocommit) is int


@pytest.mark.asyncio
async def test_async_autocommit_setter_accepts_true_and_canonical_minus_one() -> None:
    conn = _bare_async_connection()
    conn.autocommit = True
    assert conn.autocommit is True
    conn.autocommit = -1
    assert conn.autocommit == -1
    assert type(conn.autocommit) is int


@pytest.mark.asyncio
async def test_async_autocommit_setter_rejects_float_minus_one() -> None:
    conn = _bare_async_connection()
    with pytest.raises(NotSupportedError):
        conn.autocommit = -1.0


@pytest.mark.asyncio
async def test_async_autocommit_setter_rejects_decimal_minus_one() -> None:
    conn = _bare_async_connection()
    with pytest.raises(NotSupportedError):
        conn.autocommit = Decimal("-1")
