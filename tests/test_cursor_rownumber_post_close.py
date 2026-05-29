"""``Cursor.rownumber`` returns ``None`` after ``close()`` (even with a prior result set)
and ``0`` on an empty result set before any fetch.

The post-close ``None`` is coupled through ``close()``'s ``_description = None`` scrub plus
the property's ``_description is None`` branch; pin the boundary so the coupling is explicit.
"""

from __future__ import annotations

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


class _FakeSyncConn:
    def _check_thread(self) -> None: ...

    def _run_sync(self, coro: object) -> None:  # pragma: no cover - close() path
        # close() may schedule a finalize coroutine; closing it without
        # running is fine for a purely in-memory cursor.
        coro.close()  # type: ignore[attr-defined]


class _FakeAsyncConn:
    pass


def test_sync_rownumber_returns_zero_on_empty_resultset_before_fetch() -> None:
    """Index is 0 on an empty result set with a SELECT-style description populated."""
    cursor = Cursor(_FakeSyncConn())  # type: ignore[arg-type]
    cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
    cursor._rows = []
    assert cursor.rownumber == 0
    assert cursor.fetchone() is None
    assert cursor.rownumber == 0


def test_sync_rownumber_returns_none_after_close_with_active_resultset() -> None:
    cursor = Cursor(_FakeSyncConn())  # type: ignore[arg-type]
    cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
    cursor._rows = [(1,), (2,)]
    cursor.fetchone()
    assert cursor.rownumber == 1  # mid-result-set position

    cursor.close()
    assert cursor.rownumber is None


def test_async_rownumber_returns_zero_on_empty_resultset_before_fetch() -> None:
    cursor = AsyncCursor(_FakeAsyncConn())  # type: ignore[arg-type]
    cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
    cursor._rows = []
    assert cursor.rownumber == 0


def test_async_rownumber_returns_none_after_close_with_active_resultset() -> None:
    cursor = AsyncCursor(_FakeAsyncConn())  # type: ignore[arg-type]
    cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
    cursor._rows = [(1,), (2,)]
    # Manually scrub the closed state the way close() would, keeping this a
    # pure unit test for the property rather than an integration test.
    cursor._closed = True
    cursor._description = None
    cursor._row_index = 0
    assert cursor.rownumber is None
