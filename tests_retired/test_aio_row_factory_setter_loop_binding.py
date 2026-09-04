"""``AsyncConnection.row_factory`` setter checks loop binding so a foreign-loop caller
cannot silently mutate ``_row_factory`` shared with cursors on the legitimate loop."""

from __future__ import annotations

import asyncio
import threading

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


async def test_row_factory_setter_succeeds_on_bound_loop() -> None:
    """Setting from the bound loop is the happy path; the loop-binding check must not
    regress it."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        # _check_loop_binding is a no-op when not yet bound.
        aconn.row_factory = lambda cur, row: list(row)
        assert aconn._row_factory is not None
    finally:
        aconn.force_close_transport()


def test_row_factory_setter_raises_on_foreign_loop() -> None:
    """A foreign-loop caller setting ``row_factory`` raises a PEP 249 ``Error`` subclass
    instead of silently mutating shared state."""
    # Loop A's asyncio.run closes its loop on exit, so the bound loop is closed-or-GC'd
    # by the time the setter runs -> InterfaceError; a live-but-different loop would give
    # ProgrammingError. Accept either subclass of Error.
    aconn = AsyncConnection("127.0.0.1:9999")

    async def _bind_then_release() -> None:
        aconn._ensure_locks()

    asyncio.run(_bind_then_release())

    error_holder: list[BaseException] = []

    def _try_set_from_foreign_loop() -> None:  # different loop, different thread
        async def _setter() -> None:
            try:
                aconn.row_factory = lambda cur, row: row
            except BaseException as e:
                error_holder.append(e)

        asyncio.run(_setter())

    t = threading.Thread(target=_try_set_from_foreign_loop)
    t.start()
    t.join(timeout=2.0)
    assert not t.is_alive()

    aconn.force_close_transport()

    assert error_holder, (
        "expected InterfaceError/ProgrammingError from foreign-loop row_factory "
        "setter; got no exception"
    )
    assert isinstance(error_holder[0], (InterfaceError, ProgrammingError)), (
        f"expected InterfaceError or ProgrammingError, got "
        f"{type(error_holder[0]).__name__}: {error_holder[0]}"
    )


def test_row_factory_setter_value_validation_still_runs() -> None:
    """Callable-or-None validation still fires: the loop-binding check runs first but
    does not skip validation on the bound-loop happy path."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        with pytest.raises(ProgrammingError, match="must be callable or None"):
            aconn.row_factory = "not callable"
    finally:
        aconn.force_close_transport()
