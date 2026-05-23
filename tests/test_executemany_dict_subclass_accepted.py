"""Pin: ``executemany`` accepts dict subclasses (``OrderedDict``,
``defaultdict``, ``Counter``) as ordered iterables of parameter sets,
matching the documented intent of the rejection rule which calls out
only literal ``dict`` as the single-row misuse pattern.

Before this fix the reject tuple included ``dict``, which fired for
every dict subclass via ``isinstance(seq, dict)``. The comment
promised ``OrderedDict([(0, params0), (1, params1)])``-of-rows works;
the code did not deliver that. Switched to ``type(seq) is dict``
exact-type check so subclasses pass through.
"""

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
    """``OrderedDict`` is a dict subclass; the shape validator must
    accept it (the rejection rule's comment names it as the canonical
    legitimate input)."""
    od: OrderedDict[int, tuple[int]] = OrderedDict([(0, (1,)), (1, (2,))])
    # No raise: the shape validator passes through.
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
    """End-to-end: a cursor that calls executemany with an OrderedDict
    does NOT raise at the shape-validator boundary. The inner
    parameter binding may still fail (the cursor isn't dialled in
    this fixture); but the shape gate must not be the failure."""
    cur = _sync_cursor()
    od: OrderedDict[int, tuple[int]] = OrderedDict([(0, (1,)), (1, (2,))])
    # The shape validator passes; subsequent execute fails on the
    # un-dialled connection, but NOT with the "not OrderedDict" message.
    with pytest.raises(Exception) as excinfo:  # noqa: BLE001
        cur.executemany("INSERT INTO t VALUES (?)", od)
    assert "OrderedDict" not in str(excinfo.value), (
        f"OrderedDict-of-rows must not be rejected at the shape gate; got: {excinfo.value!r}"
    )
