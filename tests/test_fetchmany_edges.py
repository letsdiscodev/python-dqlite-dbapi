"""PEP 249 fetchmany edge cases (ISSUE-25)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.cursor import Cursor


def _seeded_cursor(rows: list[tuple[int, ...]]) -> Cursor:
    """Build a Cursor with ``rows`` already materialised as a result set.

    Bypasses the event-loop layer — these are pure fetch-path edge
    cases that don't need a live cluster.
    """
    conn = MagicMock()
    conn._get_async_connection = AsyncMock()
    conn._run_sync = MagicMock()
    c = Cursor(conn)
    c._rows = rows  # type: ignore[assignment]
    c._row_index = 0
    c._description = [("id", None, None, None, None, None, None)]
    c._rowcount = len(rows)
    return c


class TestFetchmanyEdges:
    def test_fetchmany_zero_returns_empty(self) -> None:
        c = _seeded_cursor([(1,), (2,), (3,)])
        assert c.fetchmany(0) == []
        # didn't advance
        assert c._row_index == 0

    def test_fetchmany_larger_than_remaining_returns_all_remaining(self) -> None:
        c = _seeded_cursor([(1,), (2,), (3,)])
        assert c.fetchmany(100) == [(1,), (2,), (3,)]
        assert c._row_index == 3

    def test_fetchmany_default_uses_arraysize(self) -> None:
        c = _seeded_cursor([(1,), (2,), (3,)])
        c.arraysize = 2
        assert c.fetchmany() == [(1,), (2,)]

    def test_for_row_in_cursor_iterates_all(self) -> None:
        c = _seeded_cursor([(1,), (2,), (3,)])
        collected = list(c)
        assert collected == [(1,), (2,), (3,)]

    def test_fetchone_then_fetchmany_continues(self) -> None:
        c = _seeded_cursor([(1,), (2,), (3,)])
        assert c.fetchone() == (1,)
        assert c.fetchmany(10) == [(2,), (3,)]

    def test_fetchall_after_partial_fetch(self) -> None:
        c = _seeded_cursor([(1,), (2,), (3,)])
        c.fetchone()
        assert c.fetchall() == [(2,), (3,)]

    def test_fetch_on_no_result_set_raises(self) -> None:
        conn = MagicMock()
        c = Cursor(conn)
        # No execute called → description is None.
        with pytest.raises(Exception, match="No result set"):
            c.fetchone()
