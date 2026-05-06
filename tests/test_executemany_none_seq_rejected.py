"""Pin: ``Cursor.executemany(sql, None)`` raises ``ProgrammingError``
(a PEP 249 ``Error`` subclass), not bare ``TypeError`` from the
iteration site.

PEP 249 §7 expects driver-raised exceptions to subclass ``Error``.
``None`` for the outer ``seq_of_parameters`` has no defensible "no
params" reading (unlike ``execute(sql, None)`` where ``None``
canonically means "no parameters"). The driver's existing strict
input-validation discipline (str/bytes/Mapping/set rejection in
``_reject_non_sequence_params``) argues for closing this last hole
so cross-driver code that wraps the call in
``except dbapi.Error:`` catches uniformly.
"""

from __future__ import annotations

import os
import threading
from typing import Any, cast

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.connection import Connection as SyncConnection
from dqlitedbapi.exceptions import Error, ProgrammingError


def _sync_cursor() -> Any:
    """Construct a Connection / Cursor without dialing — sufficient
    to drive the up-front validation on ``executemany``."""
    conn = cast(Any, SyncConnection.__new__(SyncConnection))
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._cursors = []
    cur = cast(Any, dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor))
    cur._closed = False
    cur._connection = conn
    cur.messages = []
    return cur


def _bare_async_cursor() -> Any:
    """Construct an AsyncConnection / AsyncCursor without dialing.
    Production attribute names are ``_loop_ref`` (a weakref) and
    ``_creator_pid`` — the original test fixture used ``_bound_loop_ref``
    (which is the CLIENT-layer DqliteConnection attribute, not the
    dbapi-layer AsyncConnection attribute) and ``_loop`` (also wrong).
    Both names existed on neither layer of AsyncConnection's actual
    implementation; production never read either one. The test passed
    only because the None-rejection check fires before any binding-
    check would AttributeError on the wrong-name attribute."""
    aconn = cast(Any, dqlitedbapi.aio.AsyncConnection.__new__(dqlitedbapi.aio.AsyncConnection))
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    acur = cast(Any, dqlitedbapi.aio.AsyncCursor.__new__(dqlitedbapi.aio.AsyncCursor))
    acur._closed = False
    acur._connection = aconn
    acur._executing_task = None
    acur.messages = []
    return acur


def test_sync_executemany_none_raises_programming_error() -> None:
    cur = _sync_cursor()
    with pytest.raises(ProgrammingError, match="None") as ei:
        cur.executemany("INSERT INTO t VALUES (?)", None)
    # PEP 249 §7: must be in the Error hierarchy.
    assert isinstance(ei.value, Error)


@pytest.mark.asyncio
async def test_async_executemany_none_raises_programming_error() -> None:
    acur = _bare_async_cursor()
    with pytest.raises(ProgrammingError, match="None") as ei:
        await acur.executemany("INSERT INTO t VALUES (?)", None)
    assert isinstance(ei.value, Error)
