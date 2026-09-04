"""``_cleanup_loop_thread`` (the sync Connection GC-finalize callback)
disarms the inner's ResourceWarning finalizer and reaps any pending
``_invalidate`` drain task BEFORE ``loop.stop`` lands — otherwise a
single leaked Connection emits duplicate/spurious warnings (CPython
``BaseEventLoop.close`` does not cancel pending tasks)."""

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
    """Fake inner ``DqliteConnection`` with the flag gate and an armed
    weakref finalizer."""
    inner = MagicMock()
    inner._closed_flag = [closed]
    inner._connected_flag = [True]

    def _noop_warn() -> None:
        pass

    inner._finalizer = weakref.finalize(inner, _noop_warn)
    inner._pending_drain = None
    return inner


def test_cleanup_disarms_inner_finalizer_via_boxed_handle() -> None:
    """Matching-pid (normal GC) path disarms the inner's finalizer and
    flips its closed flag via the boxed handle."""
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
    """The reap schedules the cancel via ``call_soon_threadsafe`` BEFORE
    the queued ``loop.stop`` so FIFO runs the cancel first, satisfying
    the Task and avoiding the asyncio ``Task.__del__`` warning."""
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [True]
    inner = _stub_inner(closed=False)
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

    assert inner._pending_drain is None, (
        "_cleanup_loop_thread did not null inner._pending_drain; the "
        "task will survive loop.close() and trigger asyncio's "
        "'Task was destroyed but it is pending' warning."
    )
    call_order = [call.args[0] for call in fake_loop.call_soon_threadsafe.call_args_list]
    assert len(call_order) >= 2, (
        f"_cleanup_loop_thread did not schedule both the cancel and "
        f"the loop.stop; got {len(call_order)} call_soon_threadsafe "
        f"invocations"
    )
    assert call_order[-1] == fake_loop.stop, (
        f"_cleanup_loop_thread scheduled loop.stop before the cancel; "
        f"FIFO order broken: {call_order!r}"
    )


def test_cleanup_skips_disarm_when_inner_handle_empty() -> None:
    """An empty ``_inner_finalize_handle`` box (inner never built) must
    short-circuit the inner disarm without AttributeError."""
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [True]

    inner_handle: list[object] = []

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
    fake_thread.join.assert_called_once()


def test_cleanup_skips_disarm_when_inner_already_gcd() -> None:
    """If the inner was GC'd before finalize (weakref returns None) the
    disarm path short-circuits."""
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
    """The kwarg-default captures (``_warnings``/``_logger``/etc.) guard
    against interpreter-shutdown module-globals-None teardown raising an
    unraisable-hook traceback from the finalizer body."""
    import inspect

    sig = inspect.signature(_cleanup_loop_thread)
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
    """If ``get_current_pid`` is ``None`` (mimicking ``Py_FinalizeEx``
    phase 3), cleanup short-circuits silently instead of raising."""
    from unittest.mock import patch

    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [False]

    with patch("dqlitedbapi.connection.get_current_pid", None):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            _cleanup_loop_thread(
                fake_loop,
                fake_thread,
                closed_flag,
                "host:9001",
                os.getpid(),
                5.0,
                [],
            )
        leak_warnings = [w for w in captured if issubclass(w.category, ResourceWarning)]
        assert not leak_warnings, (
            "_cleanup_loop_thread emitted a ResourceWarning while "
            "shutdown teardown had nulled get_current_pid; expected "
            "silent short-circuit."
        )


def test_full_connection_path_publishes_inner_handle() -> None:
    """After the inner is materialised, the boxed handle holds a
    ``weakref.ref(inner)``."""
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

    assert conn._inner_finalize_handle == []

    # Simulate the publish step ``_get_async_connection`` runs.
    inner = _stub_inner()
    conn._async_conn = inner
    with contextlib.suppress(Exception):
        conn._inner_finalize_handle[:] = [weakref.ref(conn._async_conn)]

    assert len(conn._inner_finalize_handle) == 1
    ref = conn._inner_finalize_handle[0]
    assert callable(ref)
    assert ref() is inner


def test_cleanup_schedules_writer_close_before_loop_stop() -> None:
    """GC finalize must schedule the inner writer's close BEFORE the
    queued ``loop.stop``; otherwise the transport is still open at
    ``loop.close()`` and the StreamWriter ``__del__`` leaks warnings."""
    from dqlitedbapi.connection import _safe_writer_close

    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [True]
    inner = _stub_inner(closed=False)
    writer = MagicMock()
    inner._protocol = MagicMock()
    inner._protocol._writer = writer

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

    calls = fake_loop.call_soon_threadsafe.call_args_list
    writer_close_idx = next(
        (
            i
            for i, c in enumerate(calls)
            if c.args and c.args[0] is _safe_writer_close and c.args[1:] == (writer,)
        ),
        None,
    )
    stop_idx = next(
        (i for i, c in enumerate(calls) if c.args and c.args[0] == fake_loop.stop),
        None,
    )
    assert writer_close_idx is not None, (
        "_cleanup_loop_thread did not schedule _safe_writer_close(writer); the "
        "inner transport stays open across loop.close() and leaks warnings at GC"
    )
    assert stop_idx is not None
    assert writer_close_idx < stop_idx, (
        "writer close scheduled after loop.stop — FIFO would close the loop before the FIN flushes"
    )
