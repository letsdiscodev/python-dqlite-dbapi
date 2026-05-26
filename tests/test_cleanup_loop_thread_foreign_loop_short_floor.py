"""Pin: when the sync ``Connection``'s teardown runs on a thread that
ALSO hosts a running asyncio loop, the daemon-thread ``thread.join``
budget shortens so the foreign loop is not parked for the full
``close_timeout``.

Trigger conditions are narrow but real:
- A test fixture or mixed-deployment app constructs a sync
  ``Connection`` from inside ``asyncio.run(...)`` on the test/app
  loop thread.
- The strong reference is dropped while the loop is still running.
- The ``weakref.finalize`` callback fires on that same loop thread.

Pre-fix the finalizer joined the daemon thread with budget
``max(close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS)`` (default 0.5 s,
floor 0.1 s). ``threading.Thread.join(timeout=...)`` is a blocking
call that parks the calling thread for the full budget — every
coroutine on the user's loop is frozen for up to 0.5 s. With the
fix, the finalizer detects the foreign-loop condition via
``asyncio.get_running_loop()`` and shortens the budget to
``_LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS`` (20 ms), bounded by
the same constant on the symmetric ``force_close_transport`` path.

The daemon thread is ``daemon=True`` so it still exits at
interpreter shutdown even if the shortened budget elapses before
``loop.stop`` lands. The sync-only deployment (no loop on the
finalizer thread) continues to get the full configured budget.
"""

from __future__ import annotations

from dqliteclient import get_current_pid
from dqlitedbapi import connection as _conn_mod


def test_foreign_loop_floor_constant_is_defined() -> None:
    """The constant must exist so the shortened-floor path can name it."""
    assert hasattr(_conn_mod, "_LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS")
    value = _conn_mod._LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS
    # Tight enough that a user-loop block is imperceptible (well under
    # one frame at 50 fps), generous enough that the daemon loop has a
    # chance to land its queued stop on a busy runner.
    assert 0.001 <= value <= 0.1, (
        f"_LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS={value} outside the sensible 1ms–100ms band"
    )


def test_cleanup_loop_thread_join_budget_for_foreign_loop_thread() -> None:
    """When the finalizer fires on a thread that hosts a running asyncio
    loop, the recorded ``thread.join`` budget must use the foreign-loop
    floor, NOT the normal ``max(close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS)``
    budget. Otherwise the user's loop is parked for the full budget.
    """
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
    inner_handle: list[object] = []  # empty: no inner connection late-published

    async def runner() -> None:
        # Invoke the finalizer body from inside a coroutine so the
        # ``asyncio.get_running_loop()`` probe inside the finalizer
        # observes a live loop on this thread.
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
    """Sync-only deployment (no loop on the finalizer thread): the
    full ``max(close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS)`` budget
    must still apply so a slow ``loop.stop`` landing has time.
    """
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

    # Run on a fresh non-loop thread so ``asyncio.get_running_loop()``
    # raises and the full budget is selected.
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
    """The symmetric ``force_close_transport`` path applies the same
    shortened floor. SA's sync ``do_close`` / ``do_terminate`` can
    reach this from a thread that hosts a running loop (mixed
    deployment).
    """
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
        # Build a stub Connection that bypasses the real __init__ —
        # we only need to exercise ``force_close_transport``'s
        # ``thread.join`` call on the loop thread.
        conn = Connection.__new__(Connection)
        conn.messages = []  # PEP 249 .messages attr cleared at top of method
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
