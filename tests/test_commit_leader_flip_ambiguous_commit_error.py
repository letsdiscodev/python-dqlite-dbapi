"""Pin: only a *leadership-lost* code rewraps a COMMIT as AmbiguousCommitError.

LEADERSHIP_LOST is emitted from the raft apply callback (after the entry was
submitted), so the write may or may not have replicated — retrying non-idempotent
DML risks silent duplicate writes. NOT_LEADER is a clean pre-apply rejection (the
write definitely did not apply) and stays a plain OperationalError. AmbiguousCommitError
inherits from OperationalError so existing catches still fire. The cursor COMMIT path
mirrors Connection.commit().
"""

from __future__ import annotations

import asyncio
import os
import weakref
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dqliteclient.exceptions as _client_exc
import dqlitedbapi
from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import (
    AMBIGUOUS_COMMIT_CODES,
    AmbiguousCommitError,
    OperationalError,
)
from dqlitewire import LEADER_ERROR_CODES

_IN_DOUBT_CODES = sorted(AMBIGUOUS_COMMIT_CODES)
_NOT_LEADER_CODES = sorted(LEADER_ERROR_CODES - AMBIGUOUS_COMMIT_CODES)


def _make_async_conn() -> AsyncConnection:
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._timeout = 5.0
    aconn._close_timeout = 1.0
    aconn.messages = []
    aconn._transaction_owner = None
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = weakref.ref(asyncio.get_running_loop())
    aconn._op_lock = asyncio.Lock()
    aconn._connect_lock = asyncio.Lock()
    inner = MagicMock()
    inner._protocol = MagicMock()
    inner.in_transaction = True
    aconn._async_conn = inner
    return aconn


def _set_commit_error(aconn: AsyncConnection, code: int) -> None:
    async def _raise(_sql: str) -> None:
        raise OperationalError("ioerr / leader", code=code, raw_message="ioerr / leader")

    aconn._async_conn.execute = _raise  # type: ignore[union-attr]


@pytest.mark.parametrize("code", _IN_DOUBT_CODES)
async def test_async_commit_leadership_lost_is_ambiguous(code: int) -> None:
    aconn = _make_async_conn()
    _set_commit_error(aconn, code)
    with pytest.raises(AmbiguousCommitError) as ei:
        await aconn.commit()
    assert isinstance(ei.value, OperationalError)  # still catches as OperationalError
    assert ei.value.code == code
    assert isinstance(ei.value.__cause__, OperationalError)


@pytest.mark.parametrize("code", _NOT_LEADER_CODES)
async def test_async_commit_not_leader_is_plain_operational(code: int) -> None:
    aconn = _make_async_conn()
    _set_commit_error(aconn, code)
    with pytest.raises(OperationalError) as ei:
        await aconn.commit()
    assert not isinstance(ei.value, AmbiguousCommitError)
    assert ei.value.code == code


def _make_sync_conn() -> Connection:
    conn = Connection("localhost:9001")
    inner = MagicMock()
    inner.close = AsyncMock()
    inner.execute = AsyncMock()
    inner._in_use = False
    inner._bound_loop = None
    conn._async_conn = inner
    return conn


@pytest.mark.parametrize("code", _IN_DOUBT_CODES)
def test_sync_commit_leadership_lost_is_ambiguous(code: int) -> None:
    conn = _make_sync_conn()
    try:
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            "leadership lost", code
        )
        with pytest.raises(AmbiguousCommitError) as ei:
            conn.commit()
        assert ei.value.code == code
    finally:
        conn._closed = True


@pytest.mark.parametrize("code", _NOT_LEADER_CODES)
def test_sync_commit_not_leader_is_plain_operational(code: int) -> None:
    conn = _make_sync_conn()
    try:
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            "not leader", code
        )
        with pytest.raises(OperationalError) as ei:
            conn.commit()
        assert not isinstance(ei.value, AmbiguousCommitError)
        assert ei.value.code == code
    finally:
        conn._closed = True


@pytest.mark.parametrize("code", _IN_DOUBT_CODES)
def test_cursor_commit_leadership_lost_is_ambiguous(code: int) -> None:
    conn = _make_sync_conn()
    try:
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            "leadership lost", code
        )
        cur = conn.cursor()
        with pytest.raises(AmbiguousCommitError) as ei:
            cur.execute("COMMIT")
        assert ei.value.code == code
    finally:
        conn._closed = True


@pytest.mark.parametrize("code", _NOT_LEADER_CODES)
def test_cursor_commit_not_leader_is_plain_operational(code: int) -> None:
    conn = _make_sync_conn()
    try:
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            "not leader", code
        )
        cur = conn.cursor()
        with pytest.raises(OperationalError) as ei:
            cur.execute("COMMIT")
        assert not isinstance(ei.value, AmbiguousCommitError)
        assert ei.value.code == code
    finally:
        conn._closed = True


@pytest.mark.parametrize("code", _IN_DOUBT_CODES)
def test_cursor_insert_leadership_lost_is_not_remapped(code: int) -> None:
    """The remap is scoped to explicit COMMIT; a bare DML leader-loss stays Operational."""
    conn = _make_sync_conn()
    try:
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            "leadership lost", code
        )
        cur = conn.cursor()
        with pytest.raises(OperationalError) as ei:
            cur.execute("INSERT INTO t VALUES (1)")
        assert not isinstance(ei.value, AmbiguousCommitError)
        assert ei.value.code == code
    finally:
        conn._closed = True


def _make_async_cursor(code: int) -> AsyncCursor:
    """An AsyncCursor whose underlying connection.execute raises a client leader error."""
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = 0
    cur._executing_task = None

    inner = MagicMock()

    async def _raise(_sql: str, _params: object) -> None:
        raise _client_exc.OperationalError("leader", code)

    inner.execute = _raise

    conn = MagicMock()
    conn._max_total_rows = None

    async def _ensure() -> object:
        return inner

    conn._ensure_connection = _ensure
    cur._connection = conn
    return cur


@pytest.mark.parametrize("code", _IN_DOUBT_CODES)
async def test_async_cursor_commit_leadership_lost_is_ambiguous(code: int) -> None:
    cur = _make_async_cursor(code)
    with (
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(AmbiguousCommitError) as ei,
    ):
        await cur._execute_unlocked("COMMIT", None)
    assert isinstance(ei.value, OperationalError)
    assert ei.value.code == code


@pytest.mark.parametrize("code", _NOT_LEADER_CODES)
async def test_async_cursor_commit_not_leader_is_plain_operational(code: int) -> None:
    cur = _make_async_cursor(code)
    with (
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(OperationalError) as ei,
    ):
        await cur._execute_unlocked("COMMIT", None)
    assert not isinstance(ei.value, AmbiguousCommitError)
    assert ei.value.code == code


@pytest.mark.parametrize("code", _IN_DOUBT_CODES)
async def test_async_cursor_insert_leadership_lost_is_not_remapped(code: int) -> None:
    """The remap is scoped to explicit COMMIT; a bare DML leader-loss stays Operational."""
    cur = _make_async_cursor(code)
    with (
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(OperationalError) as ei,
    ):
        await cur._execute_unlocked("INSERT INTO t VALUES (1)", None)
    assert not isinstance(ei.value, AmbiguousCommitError)
    assert ei.value.code == code


def test_ambiguous_commit_error_is_exported_at_package_level() -> None:
    assert dqlitedbapi.AmbiguousCommitError is AmbiguousCommitError
    assert "AmbiguousCommitError" in dqlitedbapi.__all__
