"""Pin: ``Connection.autocommit`` setter accepts the stdlib
``sqlite3.LEGACY_TRANSACTION_CONTROL`` sentinel (``-1``) in
addition to ``True``.

Stdlib 3.12+ uses the sentinel as the "do not change isolation"
signal that cross-driver code passes through. Without this
acceptance, callers porting from stdlib hit a spurious
``NotSupportedError``.

Stdlib itself enforces a similarly strict gate (only ``True`` /
``False`` / ``LEGACY_TRANSACTION_CONTROL``); we mirror that —
no truthy coercion.
"""

from __future__ import annotations

import sqlite3

import pytest

import dqlitedbapi
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import NotSupportedError


def test_sync_autocommit_accepts_true() -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = True
    finally:
        conn._closed = True


def test_sync_autocommit_accepts_legacy_transaction_control_sentinel() -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL
    finally:
        conn._closed = True


@pytest.mark.parametrize("value", [False, 0, 1, "yes", []])
def test_sync_autocommit_rejects_non_true_non_sentinel(value: object) -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        with pytest.raises(NotSupportedError, match="autocommit"):
            conn.autocommit = value
    finally:
        conn._closed = True


@pytest.mark.asyncio
async def test_async_autocommit_accepts_legacy_transaction_control_sentinel() -> None:
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    aconn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [False, 0, 1, "yes"])
async def test_async_autocommit_rejects_non_true_non_sentinel(value: object) -> None:
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    with pytest.raises(NotSupportedError, match="autocommit"):
        aconn.autocommit = value


def test_module_exports_legacy_transaction_control_sentinel_via_stdlib() -> None:
    """Sanity: the sentinel value is the stdlib symbol we accept."""
    assert sqlite3.LEGACY_TRANSACTION_CONTROL == -1
    assert dqlitedbapi.apilevel == "2.0"
