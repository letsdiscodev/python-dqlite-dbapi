"""Pin: ``Connection.__exit__`` / ``AsyncConnection.__aexit__``
short-circuit when ``self._closed is True`` (in addition to the
existing ``self._async_conn is None`` short-circuit).

Without the ``_closed`` arm, a foreign-thread
``force_close_transport`` mid-``with`` block leaves the slot at
``_async_conn is not None`` (true at the moment ``__exit__`` enters,
race-window before the force_close lands) AND eventually with
``_closed = True``. The subsequent ``self.commit()`` call from
``__exit__``'s clean-exit arm hits the ``InterfaceError`` from the
closed-state guard and supplants the body exception (or the clean
exit's success) with a closed-state misuse error.

The fix gates the entire ``__exit__`` body on ``_closed or
_async_conn is None`` so foreign-thread close races short-circuit
the context manager cleanly.
"""

from __future__ import annotations

import os
import threading
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _bare_sync_connection() -> Any:
    """Construct a Connection without dialing, with state that
    mimics a post-``force_close_transport`` shape: ``_async_conn`` is
    not None (it WAS connected during the with-block body), and
    ``_closed`` is True (force_close set this)."""
    conn = cast(Any, Connection.__new__(Connection))
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._cursors = []
    conn._async_conn = MagicMock()  # mid-with, was connected
    return conn


def test_sync_exit_short_circuits_when_closed_mid_with() -> None:
    """Simulate the foreign-thread close race: the with-block body
    enters with a live connection, then a foreign thread sets
    ``_closed = True`` and ``_async_conn = None`` (the
    force_close_transport contract). ``__exit__`` must short-circuit
    cleanly, NOT raise InterfaceError from a downstream commit."""
    conn = _bare_sync_connection()

    # Simulate force_close mid-with (sets _closed AND _async_conn=None).
    conn._closed = True
    conn._async_conn = None

    # No exception — clean exit short-circuits.
    conn.__exit__(None, None, None)


def test_sync_exit_short_circuits_when_force_close_set_closed_but_async_conn_still_alive() -> None:
    """Edge case: the foreign thread set ``_closed = True`` but the
    ``_async_conn`` null-assignment hasn't landed yet (interleaved
    atomic writes). The ``_closed`` arm must still short-circuit so
    the subsequent commit doesn't raise."""
    conn = _bare_sync_connection()

    # _closed alone is enough to short-circuit (even if _async_conn
    # is still non-None from the foreign thread's interleaved writes).
    conn._closed = True

    conn.__exit__(None, None, None)


def test_sync_exit_with_body_exception_short_circuits_when_closed() -> None:
    """Body raised; foreign thread also closed. ``__exit__`` must
    short-circuit (rollback would raise InterfaceError on the
    closed connection)."""
    conn = _bare_sync_connection()
    conn._closed = True
    conn._async_conn = None

    body_exc = ValueError("body sentinel")
    # No raise from __exit__; the original body exception propagates
    # through Python's context-manager machinery normally.
    conn.__exit__(type(body_exc), body_exc, None)


@pytest.mark.asyncio
async def test_async_exit_short_circuits_when_closed_mid_with() -> None:
    """Async sibling: same short-circuit when force_close has set
    _closed mid-async-with."""
    from dqlitedbapi.aio.connection import AsyncConnection

    aconn = cast(Any, AsyncConnection.__new__(AsyncConnection))
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    aconn._async_conn = MagicMock()
    aconn._closed = True
    aconn._async_conn = None

    # No exception.
    await aconn.__aexit__(None, None, None)
