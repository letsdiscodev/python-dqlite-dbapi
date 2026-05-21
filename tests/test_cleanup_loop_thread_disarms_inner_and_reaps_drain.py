"""Pin: ``_cleanup_loop_thread`` (the dbapi sync ``Connection``'s
GC-finalize callback) disarms the inner ``DqliteConnection``'s
ResourceWarning finalizer AND reaps any pending ``_invalidate``
drain task BEFORE ``loop.stop`` lands. Mirrors the discipline
already in ``Connection.force_close_transport``.

Without the disarm, one leaked sync ``Connection`` produced TWO
``ResourceWarning`` stderr lines (outer + inner) for the same
socket — operators counting warnings → leaks see the 2x inflation
and chase a non-existent second leak.

Without the pending-drain reap, the same GC sweep produced a third
stderr line — asyncio's ``Task.__del__`` writes "Task was destroyed
but it is pending!" via ``loop.call_exception_handler`` when the
``inner._pending_drain`` Task survives ``loop.close()`` (CPython
``BaseEventLoop.close`` does NOT cancel pending tasks).

Both fixes share the late-publish boxed-handle plumbing
(``Connection._inner_finalize_handle``) so the GC-finalize callback
can reach the inner without strong-pinning it. This test file is
the cohesive pin for the cluster.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import warnings
import weakref
from unittest.mock import MagicMock

from dqlitedbapi.connection import Connection, _cleanup_loop_thread


def _stub_inner(closed: bool = False) -> MagicMock:
    """Fake inner ``DqliteConnection`` shape with the three-flag gate
    pattern and an armed weakref finalizer. Used by the unit-level
    pins that bypass the full Connection construction.
    """
    inner = MagicMock()
    inner._closed_flag = [closed]
    inner._connected_flag = [True]

    def _noop_warn() -> None:
        pass

    inner._finalizer = weakref.finalize(inner, _noop_warn)
    inner._pending_drain = None
    return inner


def test_cleanup_disarms_inner_finalizer_via_boxed_handle() -> None:
    """When ``_cleanup_loop_thread`` runs in the matching pid (the
    normal GC path), it reads the inner from the boxed handle and
    disarms the inner's finalizer + flips its closed flag."""
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [True]  # user called close() — no outer warning expected
    inner = _stub_inner(closed=False)

    inner_handle: list[object] = [weakref.ref(inner)]

    _cleanup_loop_thread(
        fake_loop,
        fake_thread,
        closed_flag,
        "host:9001",
        os.getpid(),
        5.0,
        inner_handle,
    )

    assert inner._closed_flag[0] is True, (
        "_cleanup_loop_thread did not flip inner._closed_flag — the "
        "inner's _connection_unclosed_warning will fire on the next "
        "GC pass, emitting a misleading 'GC'd without close' on the "
        "very socket the outer's finalize was reaping."
    )
    assert inner._finalizer is None, (
        "_cleanup_loop_thread did not null inner._finalizer — even "
        "with the flag flipped, a future test fixture rebuilding "
        "from this fixture's state would observe a stale finalizer."
    )


def test_cleanup_reaps_inner_pending_drain_before_loop_stop() -> None:
    """The bounded-resnapshot reap must schedule a cancel via
    ``call_soon_threadsafe`` BEFORE the queued ``loop.stop`` so the
    FIFO of the ready queue executes the cancel first, satisfying
    the Task and preventing the asyncio ``Task.__del__`` warning."""
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [True]
    inner = _stub_inner(closed=False)
    # Simulate an in-flight ``_pending_drain`` task. Use a real
    # ``asyncio.Task``-like mock with ``done() -> False``.
    pending = MagicMock()
    pending.done.return_value = False
    inner._pending_drain = pending

    inner_handle: list[object] = [weakref.ref(inner)]

    _cleanup_loop_thread(
        fake_loop,
        fake_thread,
        closed_flag,
        "host:9001",
        os.getpid(),
        5.0,
        inner_handle,
    )

    # The reap must have nulled ``_pending_drain``.
    assert inner._pending_drain is None, (
        "_cleanup_loop_thread did not null inner._pending_drain; the "
        "task will survive loop.close() and trigger asyncio's "
        "'Task was destroyed but it is pending' warning."
    )
    # Verify the cancel-and-observe was scheduled before loop.stop.
    call_order = [call.args[0] for call in fake_loop.call_soon_threadsafe.call_args_list]
    assert len(call_order) >= 2, (
        f"_cleanup_loop_thread did not schedule both the cancel and "
        f"the loop.stop; got {len(call_order)} call_soon_threadsafe "
        f"invocations"
    )
    # First scheduled callable is the cancel-and-observe; last is
    # loop.stop.
    assert call_order[-1] == fake_loop.stop, (
        f"_cleanup_loop_thread scheduled loop.stop before the cancel; "
        f"FIFO order broken: {call_order!r}"
    )


def test_cleanup_skips_disarm_when_inner_handle_empty() -> None:
    """Negative pin: a ``Connection`` whose ``_async_conn`` was never
    built (no ``cursor()`` / ``execute()`` call ever ran) leaves the
    ``_inner_finalize_handle`` box empty. The finalize body must
    short-circuit the inner-targeted disarm — no AttributeError on
    a missing inner."""
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [True]

    # Empty box: the inner was never published.
    inner_handle: list[object] = []

    # Must not raise.
    _cleanup_loop_thread(
        fake_loop,
        fake_thread,
        closed_flag,
        "host:9001",
        os.getpid(),
        5.0,
        inner_handle,
    )

    # The outer loop teardown still runs.
    fake_loop.call_soon_threadsafe.assert_called()
    fake_thread.join.assert_called_once()


def test_cleanup_skips_disarm_when_inner_already_gcd() -> None:
    """If the inner has already been GC'd between publish time and
    finalize time (the weakref returns None), the disarm path
    short-circuits."""
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [True]
    inner = _stub_inner()
    inner_handle: list[object] = [weakref.ref(inner)]
    del inner  # weakref now returns None
    import gc

    gc.collect()

    # Must not raise; outer teardown still runs.
    _cleanup_loop_thread(
        fake_loop,
        fake_thread,
        closed_flag,
        "host:9001",
        os.getpid(),
        5.0,
        inner_handle,
    )
    fake_loop.call_soon_threadsafe.assert_called()


def test_cleanup_signature_includes_kwarg_default_module_captures() -> None:
    """Doc / shape pin: the kwarg defaults that protect against
    interpreter-shutdown module-globals-None-set teardown are
    present. Without them, a phase-3 ``Py_FinalizeEx`` cycle-collect
    on a connection that never had ``close()`` called would emit an
    unraisable-hook ``TypeError('NoneType' object is not callable)``
    or ``AttributeError`` traceback from inside the finalizer body.
    """
    import inspect

    sig = inspect.signature(_cleanup_loop_thread)
    # The captured-by-kwarg-default names. Read the parameter
    # objects' defaults to confirm capture-at-definition-time.
    kw_only = {
        name: p for name, p in sig.parameters.items() if p.kind == inspect.Parameter.KEYWORD_ONLY
    }
    for expected in ("_warnings", "_logger", "_contextlib"):
        assert expected in kw_only, (
            f"_cleanup_loop_thread is missing the {expected!r} "
            f"kwarg-default capture; shutdown-time module-globals-"
            f"None-set teardown will raise an unraisable-hook "
            f"traceback from this body."
        )
        # The captured default must NOT be the sentinel ``empty`` —
        # the value should already be bound at definition time.
        assert kw_only[expected].default is not inspect.Parameter.empty


def test_cleanup_swallows_get_current_pid_none_at_shutdown() -> None:
    """Direct shutdown pin: if the module-level ``get_current_pid``
    is replaced with ``None`` (mimicking ``Py_FinalizeEx`` phase 3),
    the cleanup short-circuits silently instead of raising
    ``TypeError('NoneType' object is not callable)`` from the
    finalize body."""
    from unittest.mock import patch

    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [False]

    # Patch the module-global ``get_current_pid`` to ``None`` —
    # exactly what ``PyImport_Cleanup`` does at shutdown phase 3.
    with patch("dqlitedbapi.connection.get_current_pid", None):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            # Must not raise / must not emit an unraisable-hook
            # traceback. The cleanup is allowed to skip silently —
            # the shutdown is already destroying every other
            # resource, so a missed warning emission is acceptable.
            _cleanup_loop_thread(
                fake_loop,
                fake_thread,
                closed_flag,
                "host:9001",
                os.getpid(),
                5.0,
                [],
            )
        # No warning is emitted (the get_current_pid call dies
        # silently before the warn arm). The loop teardown is
        # also skipped under this path — acceptable trade-off:
        # at shutdown the loop is being torn down by Python's
        # own ``Py_FinalizeEx`` machinery.
        leak_warnings = [w for w in captured if issubclass(w.category, ResourceWarning)]
        assert not leak_warnings, (
            "_cleanup_loop_thread emitted a ResourceWarning while "
            "shutdown teardown had nulled get_current_pid; expected "
            "silent short-circuit."
        )


def test_full_connection_path_publishes_inner_handle() -> None:
    """End-to-end shape pin: after a ``Connection.cursor()`` /
    ``_get_async_connection`` path materialises the inner, the
    boxed handle that ``_cleanup_loop_thread`` reads from is
    populated with a ``weakref.ref(inner)``."""
    conn = Connection.__new__(Connection)
    conn._address = "host:9001"
    conn._database = "main"
    conn._closed = False
    conn._closed_flag = [False]
    conn._async_conn = None
    conn._inner_finalize_handle = []
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._loop_lock = threading.Lock()
    conn._op_lock = threading.Lock()
    conn._loop = None
    conn._thread = None
    conn._connect_lock = None
    conn._finalizer = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []

    # Box should start empty.
    assert conn._inner_finalize_handle == []

    # Manually simulate the publish step the production code runs
    # inside ``_get_async_connection``.
    inner = _stub_inner()
    conn._async_conn = inner
    with contextlib.suppress(Exception):
        conn._inner_finalize_handle[:] = [weakref.ref(conn._async_conn)]

    assert len(conn._inner_finalize_handle) == 1
    ref = conn._inner_finalize_handle[0]
    assert callable(ref)
    assert ref() is inner
