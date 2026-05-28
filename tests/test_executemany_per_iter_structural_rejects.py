"""Pin: ``Cursor._executemany_async`` per-iteration structural-type
rejects.

The per-iter arms in ``Cursor._executemany_async`` reject a single
``params`` row that is ``str`` / ``bytes`` / ``bytearray`` /
``memoryview`` / ``Mapping`` / ``set`` / ``frozenset``, and surface a
count-mismatch ``ProgrammingError`` when the per-iter ``len(params)``
disagrees with the hoisted placeholder count. These are distinct from
the OUTER-shape rejects covered by
``test_cursor_executemany_outer_shape_check.py`` (those reject the
seq_of_parameters container itself before iteration begins).

Without per-iter unit coverage:

1. A future refactor that consolidates the structural rejects into the
   single-execute path could silently drop the executemany arms without
   test failure.
2. The "sharp structural diagnostic BEFORE the misleading per-character
   count" invariant ``_executemany_async`` commits to is not pinned
   for the per-iter loop.

The tests drive ``_executemany_async`` directly so the per-iter reject
arms are reached without the wire / dial / op_lock plumbing.

The async cursor (``AsyncCursor.executemany``) carries the same explicit
per-iteration arms — structural reject (via ``_validate_caller_param_shape``)
followed by the hoisted-placeholder-count arity check — so it raises the
same ``ProgrammingError`` locally rather than letting a wrong-arity row
reach the server. That parity is exercised against a live cluster by
``tests/integration/test_async_executemany_param_count.py``. This file
pins the sync-side per-iter arms only.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


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
    conn._max_total_rows = None
    cur._connection = conn
    return cur


def _drive_coroutine_to_first_exception(coro: Any) -> BaseException:
    """Push a coroutine forward until it raises; return the exception.

    The per-iter structural rejects raise synchronously before any
    awaitable yields, so ``coro.send(None)`` reaches the raise without
    needing an event loop."""
    try:
        coro.send(None)
    except StopIteration:
        pytest.fail("coroutine completed without raising")
    except BaseException as e:
        return e
    pytest.fail("coroutine did not raise")


@pytest.mark.parametrize(
    ("bad_row", "match"),
    [
        ("abc", "parameters must be a sequence of values"),
        (b"abc", "parameters must be a sequence of values"),
        (bytearray(b"abc"), "parameters must be a sequence of values"),
        (memoryview(b"abc"), "parameters must be a sequence of values"),
        ({"a": 1}, "qmark paramstyle requires a sequence; got a mapping"),
        ({1, 2}, "qmark paramstyle requires an ordered sequence; got a set"),
        (frozenset((1, 2)), "qmark paramstyle requires an ordered sequence; got a set"),
    ],
)
def test_executemany_async_per_iter_rejects_structural_shapes(bad_row: object, match: str) -> None:
    """Each structural-type per-iter row raises ``ProgrammingError``
    with the sharp diagnostic, BEFORE reaching the wire and BEFORE the
    misleading per-character count-mismatch arm."""
    cur = _prime_sync_cursor()
    # ``_executemany_async`` accepts ``Iterable[Sequence[Any]]`` per its
    # signature, but the per-iter reject arms exist precisely to catch
    # non-Sequence rows (str/bytes/Mapping/set/frozenset). ``Any`` cast
    # is appropriate for the deliberate misuse.
    coro = cur._executemany_async("INSERT INTO t VALUES (?)", iter([bad_row]))  # type: ignore[arg-type]
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert match in str(exc)
    # No iteration completed successfully — the structural reject
    # fired before ``_execute_async`` ran.
    assert cur._completed_iterations == 0


def test_executemany_async_per_iter_count_mismatch_after_structural_pass() -> None:
    """A correctly-shaped (sequence) row with the wrong arity hits the
    count-mismatch arm at L1726. ``_completed_iterations`` remains 0
    because the count check runs BEFORE the iteration's
    ``_execute_async`` call."""
    cur = _prime_sync_cursor()
    coro = cur._executemany_async("INSERT INTO t VALUES (?)", iter([(1, 2, 3)]))
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert "Incorrect number of bindings supplied" in str(exc)
    assert "uses 1, and there are 3 supplied" in str(exc)
    assert cur._completed_iterations == 0
