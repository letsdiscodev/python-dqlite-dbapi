"""Pin: caller-shape parameter rejection (``ProgrammingError``) preserves prior result-set
state. Stdlib only scrubs on prepare-stage rejections; the structural reject must run ABOVE
``_reset_execute_state`` so a retry-with-coerce idiom can still inspect ``cur.description``.
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
    """Cursor with a pretend prior result set to observe whether rejection scrubs it."""
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
    """Caller-shape parameter rejection must NOT scrub cur.description."""
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
    """Negative pin: prepare-stage rejections (empty SQL, etc.) STILL scrub state."""
    cur = _sync_cursor_with_prior_result_state()

    with pytest.raises(ProgrammingError, match="empty"):
        cur.execute("")

    assert cur._description is None


def test_non_str_operation_also_preserves_state() -> None:
    """Non-str operation preserves state via its early-return ProgrammingError."""
    cur = _sync_cursor_with_prior_result_state()
    prior_description = cur._description

    with pytest.raises(ProgrammingError, match="operation must be a str"):
        cur.execute(123)

    assert cur._description == prior_description
