"""Closed / GC'd loop arms raise InterfaceError (PEP 249 §3: interface gone), so psycopg-
parity retry middleware catches them; the live-but-different loop arm stays ProgrammingError.
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
    """bound is None (weakref expired) -> InterfaceError."""
    assert _loop_affinity_exc_class(None) is InterfaceError


def test_loop_affinity_exc_class_closed_loop_is_interface_error() -> None:
    """A closed-but-not-GC'd loop -> InterfaceError."""
    dead_loop = asyncio.new_event_loop()
    dead_loop.close()
    try:
        assert _loop_affinity_exc_class(dead_loop) is InterfaceError
    finally:
        pass


def test_loop_affinity_exc_class_live_different_loop_is_programming_error() -> None:
    """A live-but-different loop -> ProgrammingError (programmer mistake)."""
    live_loop = asyncio.new_event_loop()
    try:
        assert _loop_affinity_exc_class(live_loop) is ProgrammingError
    finally:
        live_loop.close()


@pytest.mark.asyncio
async def test_check_loop_only_closed_loop_raises_interface_error() -> None:
    """_check_loop_only on a connection bound to a closed loop surfaces InterfaceError."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = None  # type: ignore[assignment]
    dead_loop = asyncio.new_event_loop()
    dead_loop.close()
    aconn._loop_ref = weakref.ref(dead_loop)

    with pytest.raises(InterfaceError, match="loop"):
        aconn._check_loop_only()
