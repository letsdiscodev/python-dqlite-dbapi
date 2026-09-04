"""``executemany`` accepts dict subclasses (OrderedDict, defaultdict, Counter) as
ordered iterables; only literal ``dict`` (exact type) is the rejected single-row misuse."""

from __future__ import annotations

import os
import threading
from collections import Counter, OrderedDict, defaultdict
from typing import Any, cast

import pytest

import dqlitedbapi
from dqlitedbapi.connection import Connection as SyncConnection
from dqlitedbapi.cursor import _validate_executemany_seq_shape
from dqlitedbapi.exceptions import ProgrammingError


def _sync_cursor() -> Any:
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


def test_ordereddict_accepted_by_shape_validator() -> None:
    """OrderedDict (a dict subclass) must pass the shape validator."""
    od: OrderedDict[int, tuple[int]] = OrderedDict([(0, (1,)), (1, (2,))])
    _validate_executemany_seq_shape(od)


def test_defaultdict_accepted_by_shape_validator() -> None:
    dd: defaultdict[int, tuple[int, ...]] = defaultdict(tuple)
    dd[0] = (1,)
    _validate_executemany_seq_shape(dd)


def test_counter_accepted_by_shape_validator() -> None:
    c: Counter[str] = Counter(["a", "b", "a"])
    _validate_executemany_seq_shape(c)


def test_literal_dict_still_rejected_by_shape_validator() -> None:
    """The single-row misuse pattern remains rejected."""
    with pytest.raises(ProgrammingError, match="not dict"):
        _validate_executemany_seq_shape({"a": 1})


def test_cursor_executemany_accepts_ordereddict_at_shape_check() -> None:
    """executemany with an OrderedDict must not fail at the shape-validator gate
    (the un-dialled connection may still fail the subsequent execute)."""
    cur = _sync_cursor()
    od: OrderedDict[int, tuple[int]] = OrderedDict([(0, (1,)), (1, (2,))])
    with pytest.raises(Exception) as excinfo:  # noqa: BLE001
        cur.executemany("INSERT INTO t VALUES (?)", od)
    assert "OrderedDict" not in str(excinfo.value), (
        f"OrderedDict-of-rows must not be rejected at the shape gate; got: {excinfo.value!r}"
    )
