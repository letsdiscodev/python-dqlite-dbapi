"""Pin: ``Cursor.rownumber`` returns ``None`` after ``close()`` even
when the cursor had an active result set, and returns ``0`` on an empty
result set before any fetch.

The post-close ``None`` semantics are coupled implicitly through
``close()``'s ``_description = None`` scrub plus the property's
``if self._description is None: return None`` branch. A future
refactor that retained ``_description`` while scrubbing ``_row_index``
(or vice versa — e.g. matching stdlib's "retain description after
close" behaviour) would silently flip the closed-cursor contract.
These tests pin the boundary so the coupling is explicit.
"""

from __future__ import annotations

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


class _FakeSyncConn:
    def _check_thread(self) -> None: ...

    def _run_sync(self, coro: object) -> None:  # pragma: no cover - close() path
        # Match the existing test_dbapi_hardening fixture: cursor.close()
        # may schedule a finalize-side coroutine through the parent
        # ``_run_sync``; closing it without running is fine for a
        # purely in-memory cursor.
        coro.close()  # type: ignore[attr-defined]


class _FakeAsyncConn:
    pass


def test_sync_rownumber_returns_zero_on_empty_resultset_before_fetch() -> None:
    """0-based index of next row is 0 even on an empty result set,
    so long as a SELECT-style description is populated. This is the
    boundary case between ``None`` (no result set / closed) and a
    positive integer (rows fetched)."""
    cursor = Cursor(_FakeSyncConn())  # type: ignore[arg-type]
    cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
    cursor._rows = []
    assert cursor.rownumber == 0
    assert cursor.fetchone() is None
    assert cursor.rownumber == 0


def test_sync_rownumber_returns_none_after_close_with_active_resultset() -> None:
    """A cursor that had an active result set returns ``None`` for
    ``rownumber`` after ``close()`` — the property's ``_description
    is None`` discriminator picks up the scrub at close()."""
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
    # Manually scrub the closed state the way ``close()`` would, so
    # this stays a pure unit test for the property's discriminator
    # rather than an integration test through the close()-coroutine
    # plumbing.
    cursor._closed = True
    cursor._description = None
    cursor._row_index = 0
    assert cursor.rownumber is None
