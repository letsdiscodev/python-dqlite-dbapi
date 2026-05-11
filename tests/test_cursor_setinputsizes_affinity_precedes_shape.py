"""Pin: on the open-cursor path, ``Cursor.setinputsizes`` /
``Cursor.setoutputsize`` (sync) and ``AsyncCursor.setinputsizes`` /
``AsyncCursor.setoutputsize`` (async) fire the thread/loop-affinity
check BEFORE shape validation.

Both branches raise ``ProgrammingError`` — diagnostic-message
correctness only. The affinity-before-shape ordering matches the
sibling open-cursor methods (``nextset`` / ``scroll`` /
``executescript`` / ``callproc``), so an operator triaging a
cross-thread / cross-loop misuse with a misshapen ``sizes`` arg
sees the "wrong thread/loop" diagnostic instead of being misled
by the shape diagnostic.

The closed-permissive-return contract (PEP 249 §6.2 "free to do
nothing") is still honoured: closed cursor + bad arg + wrong
thread → silent return (covered by the sibling
``test_cursor_setinputsizes_setoutputsize_closed_state_precedes_validation.py``).
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from dqlitedbapi import Connection, ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


def test_sync_setinputsizes_thread_affinity_precedes_shape_validation() -> None:
    conn = Connection("127.0.0.1:9001", timeout=0.5)
    try:
        cur = conn.cursor()
        holder: list[BaseException] = []

        def _caller() -> None:
            try:
                cur.setinputsizes(b"not a sequence-of-int")
            except BaseException as e:  # noqa: BLE001
                holder.append(e)

        t = threading.Thread(target=_caller)
        t.start()
        t.join()
        assert holder, "expected a ProgrammingError from cross-thread caller"
        exc = holder[0]
        assert isinstance(exc, ProgrammingError)
        # Affinity diagnostic — NOT the shape diagnostic.
        msg = str(exc).lower()
        assert "thread" in msg, (
            f"expected thread-affinity diagnostic before shape diagnostic; got {exc!r}"
        )
        assert "size hints" not in msg and "expects a sequence" not in msg, (
            f"shape diagnostic leaked before affinity check; got {exc!r}"
        )
    finally:
        conn.close()


def test_sync_setoutputsize_thread_affinity_precedes_shape_validation() -> None:
    conn = Connection("127.0.0.1:9001", timeout=0.5)
    try:
        cur = conn.cursor()
        holder: list[BaseException] = []

        def _caller() -> None:
            try:
                cur.setoutputsize("not-an-int")  # type: ignore[arg-type]
            except BaseException as e:  # noqa: BLE001
                holder.append(e)

        t = threading.Thread(target=_caller)
        t.start()
        t.join()
        assert holder, "expected a ProgrammingError from cross-thread caller"
        exc = holder[0]
        assert isinstance(exc, ProgrammingError)
        msg = str(exc).lower()
        assert "thread" in msg, (
            f"expected thread-affinity diagnostic before shape diagnostic; got {exc!r}"
        )
        assert "expects an int" not in msg, (
            f"shape diagnostic leaked before affinity check; got {exc!r}"
        )
    finally:
        conn.close()


async def test_async_setinputsizes_loop_affinity_precedes_shape_validation() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()  # bind to outer loop

    holder: list[BaseException] = []

    def _runner() -> None:
        async def _invoke() -> None:
            try:
                cur.setinputsizes(b"not a sequence-of-int")  # type: ignore[arg-type]
            except BaseException as e:  # noqa: BLE001
                holder.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    assert holder, "expected a ProgrammingError from cross-loop caller"
    exc = holder[0]
    assert isinstance(exc, ProgrammingError)
    msg = str(exc).lower()
    assert "loop" in msg, f"expected loop-binding diagnostic before shape diagnostic; got {exc!r}"
    assert "size hints" not in msg and "expects a sequence" not in msg, (
        f"shape diagnostic leaked before affinity check; got {exc!r}"
    )


async def test_async_setoutputsize_loop_affinity_precedes_shape_validation() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()

    holder: list[BaseException] = []

    def _runner() -> None:
        async def _invoke() -> None:
            try:
                cur.setoutputsize("not-an-int")  # type: ignore[arg-type]
            except BaseException as e:  # noqa: BLE001
                holder.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    assert holder, "expected a ProgrammingError from cross-loop caller"
    exc = holder[0]
    assert isinstance(exc, ProgrammingError)
    msg = str(exc).lower()
    assert "loop" in msg, f"expected loop-binding diagnostic before shape diagnostic; got {exc!r}"
    assert "expects an int" not in msg, (
        f"shape diagnostic leaked before affinity check; got {exc!r}"
    )


def test_sync_open_cursor_same_thread_bad_arg_still_raises_shape() -> None:
    """Sanity: when the affinity check passes (same thread), the shape
    validator still fires."""
    conn = Connection("127.0.0.1:9001", timeout=0.5)
    try:
        cur = conn.cursor()
        with pytest.raises(ProgrammingError) as excinfo:
            cur.setinputsizes(b"not a sequence-of-int")
        assert "size hints" in str(excinfo.value) or "expects a sequence" in str(excinfo.value)
    finally:
        conn.close()


async def test_async_open_cursor_same_loop_bad_arg_still_raises_shape() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    with pytest.raises(ProgrammingError) as excinfo:
        cur.setinputsizes(b"not a sequence-of-int")  # type: ignore[arg-type]
    assert "size hints" in str(excinfo.value) or "expects a sequence" in str(excinfo.value)
