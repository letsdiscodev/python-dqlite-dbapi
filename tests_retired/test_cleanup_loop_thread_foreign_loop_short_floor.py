"""When the sync Connection's finalizer fires on a thread hosting a
running asyncio loop (e.g. constructed inside ``asyncio.run``), the
``thread.join`` budget shortens to ``_LOOP_THREAD_JOIN_FOREIGN_FLOOR``
so the user's loop is not parked for the full ``close_timeout``.
Sync-only deployments keep the full budget."""

from __future__ import annotations

from dqliteclient import get_current_pid
from dqlitedbapi import connection as _conn_mod


def test_foreign_loop_floor_constant_is_defined() -> None:
    assert hasattr(_conn_mod, "_LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS")
    value = _conn_mod._LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS
    # Imperceptible user-loop block, yet enough for the daemon loop to
    # land its queued stop on a busy runner.
    assert 0.001 <= value <= 0.1, (
        f"_LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS={value} outside the sensible 1ms–100ms band"
    )


def test_cleanup_loop_thread_join_budget_for_foreign_loop_thread() -> None:
    """Finalizer on a loop-hosting thread uses the foreign-loop floor,
    not the normal ``max(close_timeout, _LOOP_THREAD_JOIN_MIN)``."""
    import asyncio
    import threading
    from unittest.mock import MagicMock

    join_budgets: list[float | None] = []

    fake_thread = MagicMock(spec=threading.Thread)
    fake_thread.join = MagicMock(side_effect=lambda timeout=None: join_budgets.append(timeout))

    fake_loop = MagicMock()
    fake_loop.is_closed.return_value = False
    fake_loop.call_soon_threadsafe = MagicMock()
    fake_loop.close = MagicMock()

    closed_flag = [False]  # not closed → emits the warning
    inner_handle: list[object] = []

    async def runner() -> None:
        # Run from inside a coroutine so the finalizer's
        # ``asyncio.get_running_loop()`` probe sees a live loop.
        _conn_mod._cleanup_loop_thread(
            loop=fake_loop,
            thread=fake_thread,
            closed_flag=closed_flag,
            inner_handle=inner_handle,
            creator_pid=get_current_pid(),
            address="addr",
            close_timeout=0.5,
        )

    asyncio.run(runner())

    assert join_budgets, "thread.join was never called"
    actual = join_budgets[-1]
    expected = _conn_mod._LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS
    assert actual == expected, (
        f"thread.join budget was {actual} but expected the foreign-loop "
        f"floor {expected}; without the shortened floor the user's "
        "event loop is parked for the full close_timeout"
    )


def test_cleanup_loop_thread_join_budget_for_off_loop_thread() -> None:
    """Sync-only (no loop on the finalizer thread): the full budget
    still applies so a slow ``loop.stop`` landing has time."""
    import threading
    from unittest.mock import MagicMock

    join_budgets: list[float | None] = []

    fake_thread = MagicMock(spec=threading.Thread)
    fake_thread.join = MagicMock(side_effect=lambda timeout=None: join_budgets.append(timeout))

    fake_loop = MagicMock()
    fake_loop.is_closed.return_value = False
    fake_loop.call_soon_threadsafe = MagicMock()
    fake_loop.close = MagicMock()

    closed_flag = [False]
    inner_handle: list[object] = []

    # Fresh non-loop thread so ``get_running_loop()`` raises → full budget.
    runner_thread = threading.Thread(
        target=_conn_mod._cleanup_loop_thread,
        kwargs={
            "loop": fake_loop,
            "thread": fake_thread,
            "closed_flag": closed_flag,
            "inner_handle": inner_handle,
            "creator_pid": get_current_pid(),
            "address": "addr",
            "close_timeout": 0.5,
        },
    )
    runner_thread.start()
    runner_thread.join()

    assert join_budgets, "thread.join was never called"
    actual = join_budgets[-1]
    expected = max(0.5, _conn_mod._LOOP_THREAD_JOIN_MIN_SECONDS)
    assert actual == expected, (
        f"thread.join budget was {actual} but expected the full "
        f"budget {expected}; sync-only deployments must not pay the "
        "shortened floor that exists for foreign-loop GC."
    )


def test_force_close_transport_uses_foreign_loop_floor_when_called_on_loop_thread() -> None:
    """``force_close_transport`` applies the same shortened floor (SA's
    sync do_close/do_terminate can reach it from a loop thread)."""
    import asyncio
    import threading
    import weakref
    from unittest.mock import MagicMock, patch

    from dqlitedbapi.connection import Connection

    join_budgets: list[float | None] = []

    fake_thread = MagicMock(spec=threading.Thread)
    fake_thread.join = MagicMock(side_effect=lambda timeout=None: join_budgets.append(timeout))

    fake_loop = MagicMock()
    fake_loop.is_closed.return_value = False
    fake_loop.call_soon_threadsafe = MagicMock()
    fake_loop.close = MagicMock()

    async def runner() -> None:
        conn = Connection.__new__(Connection)
        conn.messages = []  # cleared at top of force_close_transport
        conn._closed = False
        conn._closed_flag = [False]
        conn._creator_pid = get_current_pid()
        conn._cursors = weakref.WeakSet()
        conn._finalizer = None
        conn._loop_lock = threading.Lock()
        conn._async_conn = None
        conn._loop = fake_loop
        conn._thread = fake_thread
        conn._connect_lock = None
        conn._transaction_owner = None
        conn._close_timeout = 0.5

        with patch.object(Connection, "_cascade_cursors"):
            conn.force_close_transport()

    asyncio.run(runner())

    assert join_budgets, "thread.join was never called"
    actual = join_budgets[-1]
    expected = _conn_mod._LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS
    assert actual == expected, (
        f"force_close_transport thread.join budget was {actual}, "
        f"expected foreign-loop floor {expected}"
    )
