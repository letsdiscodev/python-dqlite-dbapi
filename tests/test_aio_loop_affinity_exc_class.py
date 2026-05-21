"""Pin: closed / GC'd loop arms of ``_format_loop_affinity_message``
raise ``InterfaceError`` (matches the client-layer sibling); the
live-but-different loop arm keeps ``ProgrammingError``.

PEP 249 §3 places "the database interface is gone" under
``InterfaceError``; "the user's programming is wrong" under
``ProgrammingError``. A torn-down event loop is the former. The
client-layer sibling at ``dqliteclient.connection._check_in_use``
already uses ``InterfaceError`` for both the closed-loop and
different-loop arms; the dbapi.aio layer used ``ProgrammingError``
for all three, breaking cross-driver retry middleware (psycopg
parity catches ``InterfaceError`` for reconnect).
"""

from __future__ import annotations

import asyncio
import weakref

import pytest

from dqlitedbapi.aio.connection import (
    AsyncConnection,
    _loop_affinity_exc_class,
)
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def test_loop_affinity_exc_class_gc_d_loop_is_interface_error() -> None:
    """``bound is None`` (the weakref expired) → ``InterfaceError``."""
    assert _loop_affinity_exc_class(None) is InterfaceError


def test_loop_affinity_exc_class_closed_loop_is_interface_error() -> None:
    """A closed-but-not-GC'd loop → ``InterfaceError``."""
    dead_loop = asyncio.new_event_loop()
    dead_loop.close()
    try:
        assert _loop_affinity_exc_class(dead_loop) is InterfaceError
    finally:
        # already closed; no-op
        pass


def test_loop_affinity_exc_class_live_different_loop_is_programming_error() -> None:
    """A live-but-different loop → ``ProgrammingError`` (programmer
    mistake, not infrastructure failure)."""
    live_loop = asyncio.new_event_loop()
    try:
        assert _loop_affinity_exc_class(live_loop) is ProgrammingError
    finally:
        live_loop.close()


@pytest.mark.asyncio
async def test_check_loop_only_closed_loop_raises_interface_error() -> None:
    """End-to-end pin: an ``AsyncConnection`` whose ``_loop_ref``
    points at a closed loop surfaces ``InterfaceError`` when
    ``_check_loop_only`` runs from a fresh loop. Cross-driver
    retry middleware (psycopg parity) catches ``InterfaceError``;
    ``ProgrammingError`` would escape that catch."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = None  # type: ignore[assignment]
    dead_loop = asyncio.new_event_loop()
    dead_loop.close()
    aconn._loop_ref = weakref.ref(dead_loop)

    with pytest.raises(InterfaceError, match="loop"):
        aconn._check_loop_only()
