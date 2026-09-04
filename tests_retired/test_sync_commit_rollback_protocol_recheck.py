"""Sync _commit_async/_rollback_async re-check _protocol under the op_lock (like async): a
foreign-thread invalidate that nulls _protocol after the pre-lock check raises the ambiguous-
state InterfaceError, not a plain OperationalError('Not connected').
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import InterfaceError

pytestmark = pytest.mark.asyncio


def _conn_with_invalidated_inner() -> Connection:
    conn = Connection.__new__(Connection)
    conn.messages = []
    inner = MagicMock()
    inner._protocol = None  # invalidated by a racing close()/_invalidate
    inner.in_transaction = True
    conn._async_conn = inner
    return conn


async def test_commit_async_recheck_raises_interface_error_on_invalidated_protocol() -> None:
    conn = _conn_with_invalidated_inner()
    with pytest.raises(InterfaceError, match="ambiguous"):
        await conn._commit_async()


async def test_rollback_async_recheck_raises_interface_error_on_invalidated_protocol() -> None:
    conn = _conn_with_invalidated_inner()
    with pytest.raises(InterfaceError, match="ambiguous"):
        await conn._rollback_async()
