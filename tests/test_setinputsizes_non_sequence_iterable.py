"""Pin: ``AsyncCursor.setinputsizes`` (and sync sibling) Sequence-ABC
dispatch arm.

The existing pin in ``test_audit_2026_05_coverage_gaps.py:213-217``
passes ``"not-a-sequence"`` (a ``str``), which hits the FIRST branch
(``str`` / ``bytes`` / ``bytearray`` reject at L933-936), not the
Sequence-ABC arm at L937-943. A non-Sequence input — e.g. ``int`` or
``dict`` — hits the latter, which was loosened from ``(list, tuple)``
to the structural ``Sequence`` ABC (per the comment block at L939-942)
to accept ``deque`` / ``range`` / custom Sequence subclasses for
psycopg2 / stdlib parity.

Without an ABC-specific pin, a future revert to ``(list, tuple)``
would pass the existing tests because the str-hitting test does not
exercise the ABC dispatch.
"""

from __future__ import annotations

import collections
from typing import Any

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.parametrize("bad_sizes", [42, {"k": 1}])
def test_sync_setinputsizes_rejects_non_sequence_iterable(bad_sizes: Any) -> None:
    """Sync sibling pin for the Sequence-ABC reject — ``int`` / ``dict``
    are non-str/bytes/bytearray AND non-Sequence, so they reach the
    ABC arm."""
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    with pytest.raises(ProgrammingError, match="expects a Sequence"):
        cur.setinputsizes(bad_sizes)


@pytest.mark.parametrize("bad_sizes", [42, {"k": 1}])
async def test_async_setinputsizes_rejects_non_sequence_iterable(bad_sizes: Any) -> None:
    """Async-sibling pin: ``int`` / ``dict`` reach the Sequence-ABC
    reject at L937-943, distinct from the str/bytes/bytearray branch
    at L933-936."""
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="expects a Sequence"):
        cur.setinputsizes(bad_sizes)


def test_sync_setinputsizes_accepts_deque_and_range() -> None:
    """Sequence-ABC dispatch accepts ``deque`` / ``range`` — psycopg2 /
    stdlib parity. A revert to ``(list, tuple)`` would break here."""
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    cur.setinputsizes(collections.deque([1, 2]))
    cur.setinputsizes(range(3))


async def test_async_setinputsizes_accepts_deque_and_range() -> None:
    """Async-sibling pin for the Sequence-ABC accept set."""
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    cur.setinputsizes(collections.deque([1, 2]))
    cur.setinputsizes(range(3))
