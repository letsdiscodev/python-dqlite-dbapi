"""Pin: ``Cursor.execute`` / ``AsyncCursor.execute`` preserve the
prior result-set state (``description``, ``rowcount``, ``rows``,
``_row_index``) when the call is rejected with ``ProgrammingError``
for a caller-shape parameter misuse (Mapping for qmark, set, str,
bytes, bytearray, memoryview).

Per ``Cursor.execute``'s documented contract:
    "stdlib only scrubs on prepare-stage rejections (empty SQL,
    multi-statement, NUL byte, bind-count mismatch). Match that
    split: ProgrammingError here surfaces with cursor state intact
    so a caller's retry-with-coerce idiom can inspect
    cur.description to shape the retry."

The structural-parameter rejections were running AFTER
``_reset_execute_state``, so a sibling ``cur.execute("SELECT 1");
cur.execute("SELECT ?", {"a": 1})`` sequence scrubbed
``cur.description`` between the two calls, breaking the
preservation contract. Hoist the structural reject ABOVE the reset.
"""

from __future__ import annotations

import os
import threading
from typing import Any, cast

import pytest

import dqlitedbapi
from dqlitedbapi.connection import Connection as SyncConnection
from dqlitedbapi.exceptions import ProgrammingError


def _sync_cursor_with_prior_result_state() -> Any:
    """Construct a Cursor with a pretend prior result set so we can
    observe whether the rejection scrubs it."""
    conn = cast(Any, SyncConnection.__new__(SyncConnection))
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._cursors = []
    cur = cast(Any, dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor))
    cur._closed = False
    cur._connection = conn
    cur.messages = []
    cur._description = (("id", None, None, None, None, None, None),)
    cur._rowcount = 3
    cur._lastrowid = 99
    cur._rows = [(1,), (2,), (3,)]
    cur._row_index = 1
    cur._arraysize = 1
    return cur


@pytest.mark.parametrize(
    "bad_params",
    [
        {"a": 1},  # Mapping for qmark
        {1, 2, 3},  # set
        frozenset((1, 2)),  # frozenset
        "abc",  # str
        b"abc",  # bytes
        bytearray(b"abc"),  # bytearray
        memoryview(b"abc"),  # memoryview
    ],
)
def test_caller_shape_param_rejection_preserves_prior_description(bad_params: Any) -> None:
    """Per the documented contract, a caller-shape parameter rejection
    must NOT scrub cur.description."""
    cur = _sync_cursor_with_prior_result_state()
    prior_description = cur._description
    prior_rowcount = cur._rowcount
    prior_rows = cur._rows
    prior_row_index = cur._row_index

    with pytest.raises(ProgrammingError):
        cur.execute("SELECT ?", bad_params)

    assert cur._description == prior_description, (
        f"caller-shape rejection scrubbed _description; expected "
        f"preservation per docstring contract. params={bad_params!r}"
    )
    assert cur._rowcount == prior_rowcount
    assert cur._rows == prior_rows
    assert cur._row_index == prior_row_index


def test_prepare_stage_rejection_still_scrubs_description() -> None:
    """Sibling negative pin: prepare-stage rejections (empty SQL,
    multi-statement, NUL byte, bind-count mismatch) STILL scrub
    state — those are the documented "scrub on rejection" cases."""
    cur = _sync_cursor_with_prior_result_state()

    with pytest.raises(ProgrammingError, match="empty"):
        cur.execute("")

    # Prepare-stage rejection: description was scrubbed.
    assert cur._description is None


def test_non_str_operation_also_preserves_state() -> None:
    """Symmetric pin: non-str operation already preserves state via
    its early-return ProgrammingError; pin that the discipline
    survives any future refactor."""
    cur = _sync_cursor_with_prior_result_state()
    prior_description = cur._description

    with pytest.raises(ProgrammingError, match="operation must be a str"):
        cur.execute(123)

    assert cur._description == prior_description
