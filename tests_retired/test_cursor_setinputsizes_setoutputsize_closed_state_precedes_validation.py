"""``setinputsizes`` / ``setoutputsize`` (sync and async) honour the closed-cursor no-op
(PEP 249 §6.2) regardless of argument shape: the closed short-circuit runs ahead of the
shape validators, but still after the unconditional messages-clear (§6.4).
"""

from __future__ import annotations

import os
import threading
from typing import Any, cast

import pytest

import dqlitedbapi
import dqlitedbapi.aio


def _bare_sync_cursor() -> Any:
    c = cast(Any, dqlitedbapi.Connection.__new__(dqlitedbapi.Connection))
    c._closed = True
    c._closed_flag = [True]
    c._creator_thread = threading.get_ident()
    c._creator_pid = os.getpid()
    c.messages = []
    cur = cast(Any, dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor))
    cur._connection = c
    cur._closed = True
    cur.messages = []
    return cur


def _bare_async_cursor() -> Any:
    c = cast(Any, dqlitedbapi.aio.AsyncConnection.__new__(dqlitedbapi.aio.AsyncConnection))
    c._closed = True
    c._closed_flag = [True]
    c._connected_flag = [False]
    c._async_conn = None
    c._creator_pid = os.getpid()
    c._creator_thread = threading.get_ident()
    c._loop_ref = None
    c._connect_lock = None
    c._op_lock = None
    c.messages = []
    cur = cast(Any, dqlitedbapi.aio.AsyncCursor.__new__(dqlitedbapi.aio.AsyncCursor))
    cur._connection = c
    cur._closed = True
    cur._executing_task = None
    cur.messages = []
    return cur


@pytest.mark.parametrize("bad_arg", ["oops", b"oops", bytearray(b"oops"), 42])
def test_sync_setinputsizes_closed_cursor_silent_on_any_arg(bad_arg: Any) -> None:
    cur = _bare_sync_cursor()
    cur.setinputsizes(bad_arg)
    assert cur.messages == []


def test_sync_setoutputsize_closed_cursor_silent_on_any_arg() -> None:
    cur = _bare_sync_cursor()
    cur.setoutputsize("not-an-int")
    cur.setoutputsize(10, "not-an-int")
    cur.setoutputsize(True)
    assert cur.messages == []


@pytest.mark.parametrize("bad_arg", ["oops", b"oops", bytearray(b"oops"), 42])
def test_async_setinputsizes_closed_cursor_silent_on_any_arg(bad_arg: Any) -> None:
    cur = _bare_async_cursor()
    cur.setinputsizes(bad_arg)
    assert cur.messages == []


def test_async_setoutputsize_closed_cursor_silent_on_any_arg() -> None:
    cur = _bare_async_cursor()
    cur.setoutputsize("not-an-int")
    cur.setoutputsize(True)
    assert cur.messages == []


def test_sync_setinputsizes_open_cursor_still_validates() -> None:
    """Negative-control: open cursor + bad arg still raises."""
    cur = _bare_sync_cursor()
    cur._closed = False  # open cursor
    cur._connection._closed = False
    cur._connection._closed_flag = [False]
    with pytest.raises(dqlitedbapi.ProgrammingError):
        cur.setinputsizes("oops")
    with pytest.raises(dqlitedbapi.ProgrammingError):
        cur.setoutputsize("oops")
