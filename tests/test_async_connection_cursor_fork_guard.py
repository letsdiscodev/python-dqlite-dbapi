"""Pin: ``AsyncConnection.cursor()`` enforces the fork-after-init guard
that every other public method on the class already enforces.

A forked child that calls ``aconn.cursor()`` from sync context (the
SA-greenlet glue shape, ``_loop_ref is not None`` but no running
loop in the child) previously bypassed the guard via the
``RuntimeError → pass`` arm and silently returned a live cursor
registered in the parent-pinned ``_cursors`` WeakSet. The next call
on that cursor would (in the async case) eventually raise via
``_ensure_locks`` — but the front-line return of a live wrapper is
the diagnostic foot-gun the rest of the class avoids.

Pin the front-line ``InterfaceError("used after fork; ...")`` so the
guard cannot regress.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def _make_async_connection_with_creator_pid(creator_pid: int) -> AsyncConnection:
    import weakref

    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = creator_pid
    aconn._loop_ref = None
    aconn._async_conn = None
    aconn._cursors = weakref.WeakSet()
    aconn.messages = []
    return aconn


def test_cursor_raises_when_current_pid_diverged_from_creator_pid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Patch the client-layer module-level ``_current_pid`` to simulate
    a fork. ``AsyncConnection.cursor()`` must surface InterfaceError
    rather than returning a live cursor that the user mistakes for a
    usable handle."""
    from dqliteclient import connection as _client_conn_mod

    aconn = _make_async_connection_with_creator_pid(creator_pid=99999)
    monkeypatch.setattr(_client_conn_mod, "_current_pid", 12345)

    with pytest.raises(InterfaceError, match="used after fork"):
        aconn.cursor()


def test_cursor_does_not_register_in_cursors_set_when_pid_diverged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defence pin: the cursors WeakSet must remain empty on the
    forked-child fast path. A regression that registers the cursor
    before the guard fires would surface here."""
    from dqliteclient import connection as _client_conn_mod

    aconn = _make_async_connection_with_creator_pid(creator_pid=99999)
    aconn._cursors = MagicMock()
    aconn._cursors.add = MagicMock(side_effect=AssertionError("must not register"))
    monkeypatch.setattr(_client_conn_mod, "_current_pid", 12345)

    with pytest.raises(InterfaceError, match="used after fork"):
        aconn.cursor()
    aconn._cursors.add.assert_not_called()


def test_cursor_works_when_pid_matches_creator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Positive control: same-process call still returns a live cursor."""
    from dqliteclient import connection as _client_conn_mod

    aconn = _make_async_connection_with_creator_pid(creator_pid=_client_conn_mod._current_pid)
    cur = aconn.cursor()
    assert cur is not None
    assert cur in aconn._cursors
