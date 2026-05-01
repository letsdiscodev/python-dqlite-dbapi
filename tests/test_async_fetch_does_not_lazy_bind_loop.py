"""Pin: ``AsyncCursor.fetch*`` and ``executescript`` use the
non-binding loop-affinity helper so a fresh cursor's first call
does NOT lazy-bind the connection's loop before the result-set guard
fires.

Previously, ``fetchone`` / ``fetchmany`` / ``fetchall`` routed through
``_ensure_locks()``, which lazily creates the asyncio locks and pins
``_loop_ref`` on first call. A fresh cursor whose first method ever
called was ``fetchone()`` (the "fetch before execute" misuse path)
silently bound the connection to the *current* loop before raising
``ProgrammingError("no results to fetch; execute a query first")``.
A subsequent legitimate call from a different loop then surfaced a
"different event loop" diagnostic referring to a loop the user did
not intend to bind.

Switching to ``_check_loop_binding`` (the non-binding variant) keeps
the up-front cross-loop diagnostic but does not lazy-bind on first
read — same family-of-footgun fix the no-op-shape cursor methods
(``setinputsizes`` / ``setoutputsize`` / ``callproc`` / ``nextset`` /
``scroll``) already adopted.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect

from dqlitedbapi.aio.cursor import AsyncCursor


def test_async_fetch_methods_call_non_binding_helper_not_ensure_locks() -> None:
    """Source-level pin: each fetch* method must call
    ``_check_loop_binding`` (non-binding) and must NOT call
    ``_ensure_locks`` (binding). A regression that swaps back to
    ``_ensure_locks`` reintroduces the lazy-bind footgun."""
    for method_name in ("fetchone", "fetchmany", "fetchall"):
        method = getattr(AsyncCursor, method_name)
        src = inspect.getsource(method)
        assert "_check_loop_binding" in src, (
            f"AsyncCursor.{method_name} must call _check_loop_binding"
        )
        assert "_ensure_locks" not in src, (
            f"AsyncCursor.{method_name} must not call _ensure_locks (lazy-bind footgun)"
        )


def test_async_executescript_calls_non_binding_helper_not_ensure_locks() -> None:
    """The executescript stub also keeps to the non-binding helper."""
    src = inspect.getsource(AsyncCursor.executescript)
    assert "_check_loop_binding" in src
    assert "_ensure_locks" not in src


def test_fresh_cursor_fetch_does_not_bind_connection_loop() -> None:
    """Behavioural pin: a fresh AsyncCursor whose first method ever
    called is ``fetchone()`` (no execute first) raises
    ``ProgrammingError("no results to fetch")`` WITHOUT lazy-binding
    the connection's loop. A subsequent call from a different loop
    must NOT surface a confusing "different event loop" diagnostic
    referring to a loop the user did not intend to bind.
    """
    from dqlitedbapi.aio.connection import AsyncConnection
    from dqlitedbapi.exceptions import ProgrammingError

    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = None  # not bound
    aconn._async_conn = None
    aconn._creator_pid = 0  # bypass fork check below
    import os

    aconn._creator_pid = os.getpid()

    cursor = AsyncCursor(aconn)
    cursor._description = None  # no result set
    cursor._rows = []

    loop = asyncio.new_event_loop()
    try:

        async def _drive() -> None:
            with contextlib.suppress(ProgrammingError):
                await cursor.fetchone()
            assert aconn._loop_ref is None, "fetchone on a fresh cursor must not lazy-bind the loop"

        loop.run_until_complete(_drive())
    finally:
        loop.close()
