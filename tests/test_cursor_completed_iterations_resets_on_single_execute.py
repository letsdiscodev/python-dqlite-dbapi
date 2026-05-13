"""Pin: ``Cursor.completed_iterations`` returns 0 after a single-row
``execute`` even when a prior ``executemany`` left a non-zero count.

The property's docstring promises:

    Resets to 0 at the start of every new executemany call.
    ... 0 after a never-executed cursor or a single-row execute.

The "0 after a single-row execute" half is violated today on a
cursor that had a prior successful ``executemany``: the counter
holds the stale ``len(seq_of_parameters)`` value because the reset
sits inside ``_executemany_async`` instead of in
``_reset_execute_state``. Callers using ``completed_iterations`` as
a partial-commit signal then misclassify a normal single-row
execute as a recovery scenario.

Fix: hoist the reset into ``_reset_execute_state`` so both
``execute`` and ``executemany`` (which already calls
``_reset_execute_state``) hit one source of truth.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _prime_sync_cursor() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = []
    cur._row_index = 0
    cur._description = None
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur._completed_iterations = 0
    cur.messages = []
    conn = MagicMock()
    conn._check_thread = MagicMock()
    cur._connection = conn
    return cur


def _prime_async_cursor() -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = []
    cur._row_index = 0
    cur._description = None
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur._completed_iterations = 0
    cur.messages = []
    cur._connection = MagicMock()
    return cur


def test_sync_reset_execute_state_clears_completed_iterations() -> None:
    """``_reset_execute_state`` is the single source of truth for
    per-execute state; ``completed_iterations`` must reset alongside
    ``_rowcount`` / ``_description`` so the property's documented
    contract holds for single-row ``execute`` after a prior
    ``executemany``."""
    cur = _prime_sync_cursor()
    cur._completed_iterations = 5

    cur._reset_execute_state()
    assert cur._completed_iterations == 0


def test_async_reset_execute_state_clears_completed_iterations() -> None:
    cur = _prime_async_cursor()
    cur._completed_iterations = 7

    cur._reset_execute_state()
    assert cur._completed_iterations == 0


def test_sync_execute_resets_completed_iterations_via_reset_helper() -> None:
    """End-to-end behaviour pin through the sync ``execute`` entry
    point: a single ``execute`` call after a stale counter clears the
    slot, regardless of whether the wire I/O succeeds or raises
    (we short-circuit the wire by mocking ``_run_sync``)."""
    cur = _prime_sync_cursor()
    cur._completed_iterations = 9

    # Stub the wire-bound side: _run_sync is the dispatch hatch.
    def _run_sync(coro: Any) -> None:
        coro.close()  # avoid unawaited-coroutine warning

    cur._connection._run_sync = _run_sync  # type: ignore[assignment]
    cur.execute("SELECT 1")
    assert cur._completed_iterations == 0
