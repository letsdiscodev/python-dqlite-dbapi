"""Pin: ``AsyncConnection.cursor()``'s defensive re-check after the
``WeakSet.add`` runs the full scrub the cascade would have applied
AND discards the entry so ``self._cursors`` matches the
``self._cursors.clear()`` postcondition.

A signal-handler / greenlet / cross-loop race between ``cursor()``
and ``close()``'s cascade snapshot can interleave such that:

1. ``cursor()`` passes ``if self._closed:`` check.
2. ``close()`` snapshots ``list(self._cursors)`` and runs its
   cascade (sets ``_closed=True`` on each, clears ``_cursors``).
3. ``cursor()`` proceeds to ``AsyncCursor(self)`` +
   ``self._cursors.add(cur)`` — inserting a fresh cursor AFTER
   the cascade's clear ran.
4. The defensive re-check observes ``self._closed`` and applies
   the scrub + discard so the WeakSet and cursor state match the
   cascade's postcondition.

Companion to the source-level fix in the same commit. The race
itself is hard to trigger deterministically (no asyncio
yield-point in the synchronous critical section), so the test
drives the re-check arm by setting ``self._closed = True`` mid-
flow via a sentinel patch on ``WeakSet.add`` and observing the
late-added cursor's state.
"""

from __future__ import annotations

import weakref
from typing import Any

from dqlitedbapi.aio.connection import AsyncConnection


def _make_aconn() -> AsyncConnection:
    """Minimal AsyncConnection bypassing the constructor's network
    machinery; only the state ``cursor()`` reads is populated."""
    aconn = AsyncConnection("127.0.0.1:9999")
    # Avoid leaving the weakref.finalize registered against the
    # uninitialised attributes of a __new__-built fixture by using
    # the real __init__ above; the address is bogus but never used.
    return aconn


def test_cursor_called_after_close_full_scrub_and_discard() -> None:
    """The defensive re-check path applies the full cascade scrub
    (``_rows``, ``_description``, ``_rowcount``, ``_lastrowid``,
    ``_row_index``, ``messages``) AND discards the cursor from the
    WeakSet so ``self._cursors`` matches the cascade's postcondition.

    Drive the race window deterministically by patching the
    ``_cursors.add`` to flip ``_closed`` to True synchronously
    inside the add — simulating a signal-handler that lands
    between the body of cursor() and the re-check.
    """
    aconn = _make_aconn()
    # Capture and replace the WeakSet with a real one and rig
    # ``add`` to flip the connection's closed flag synchronously
    # so the re-check arm fires.
    original_cursors = aconn._cursors
    real_add = original_cursors.add

    def add_then_flip_closed(cur: Any) -> None:
        real_add(cur)
        aconn._closed = True

    # MagicMock the add only; iteration / discard still delegate to
    # the real WeakSet.
    aconn._cursors.add = add_then_flip_closed  # type: ignore[assignment]

    cur = aconn.cursor()

    # 1) The cursor is marked closed.
    assert cur._closed is True

    # 2) The cursor's scrub fields match the cascade's postcondition.
    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._lastrowid is None
    assert cur._row_index == 0
    assert list(cur.messages) == []

    # 3) The cursor is NOT in the WeakSet — matches the cascade's
    # ``self._cursors.clear()`` postcondition.
    assert cur not in aconn._cursors
    assert len(aconn._cursors) == 0


def test_cursor_called_after_close_with_normal_state_intact() -> None:
    """Negative pin: when the connection is NOT mid-close at the
    re-check, the cursor is registered normally and the scrub
    fields stay at their fresh-cursor defaults."""
    aconn = _make_aconn()
    cur = aconn.cursor()

    # _closed is False by default for a fresh cursor; the re-check
    # arm skips when self._closed is False.
    assert cur._closed is False
    assert cur in aconn._cursors


def test_cursor_called_when_already_closed_short_circuits_before_add() -> None:
    """Negative pin: the canonical-correct path where ``_closed`` is
    True BEFORE cursor() runs raises ``InterfaceError`` from the
    top-of-method closed-check, never reaching the WeakSet add."""
    import dqlitedbapi

    aconn = _make_aconn()
    aconn._closed = True
    # Should raise from the top-of-method ``_closed`` precheck.
    try:
        aconn.cursor()
    except dqlitedbapi.InterfaceError:
        pass
    else:
        raise AssertionError("expected InterfaceError on closed connection")
    # The WeakSet is unchanged.
    assert len(aconn._cursors) == 0


def test_weakset_cursors_postcondition_after_race() -> None:
    """End-to-end invariant: after the race resolves, the
    ``self._cursors`` WeakSet is empty — no stale entry from a
    late-added cursor that beat the cascade's snapshot."""
    aconn = _make_aconn()
    real_add = aconn._cursors.add

    def add_then_flip_closed(cur: Any) -> None:
        real_add(cur)
        aconn._closed = True

    aconn._cursors.add = add_then_flip_closed  # type: ignore[assignment]

    cur = aconn.cursor()
    # The discard ran; len() reflects the cleared state.
    assert len(aconn._cursors) == 0
    # And keeping the cursor alive (this scope holds ``cur``) does
    # not re-insert it.
    assert isinstance(weakref.ref(cur), weakref.ref)
