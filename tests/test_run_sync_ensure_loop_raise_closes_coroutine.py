"""Pin: ``Connection._run_sync`` closes the input coroutine if
``_ensure_loop()`` raises before the coroutine is scheduled.

Three failure modes in ``_run_sync`` prevent ``coro`` from ever being
awaited:

1. ``self._op_lock.acquire(timeout=...)`` returns False (couldn't
   acquire). Cleanup at ``connection.py:1075`` calls ``coro.close()``.
2. ``self._ensure_loop()`` raises (e.g. ``Thread.start()`` failing
   under OS resource exhaustion, or ``asyncio.new_event_loop()``
   failing on FD ulimit exhaustion). **This was the un-cleaned-up
   path** — the bare raise propagated past ``coro``, surfacing as a
   ``RuntimeWarning("coroutine ... was never awaited")`` at GC.
3. ``asyncio.run_coroutine_threadsafe(...)`` raises ``RuntimeError``
   (loop closed mid-schedule). Cleanup at ``connection.py:1124``
   calls ``coro.close()``.

The fix wraps ``_ensure_loop()`` in its own narrow try/except so the
secondary cleanup runs without conflating ``_ensure_loop`` failures
(FD exhaustion class) with ``run_coroutine_threadsafe``'s closed-
loop RuntimeError.
"""

from __future__ import annotations

import gc
import warnings

import pytest

import dqlitedbapi


def test_run_sync_closes_coro_when_ensure_loop_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """If ``_ensure_loop()`` raises before the coroutine is scheduled,
    ``_run_sync`` must call ``coro.close()`` so no ``RuntimeWarning``
    leaks at GC time. Mirrors the other two cleanup branches in the
    same method.

    Pre-fix: a bare raise propagates past the unscheduled ``coro``,
    and at GC the runtime emits ``coroutine '...' was never
    awaited``. Under ``-W error::RuntimeWarning`` this masks the
    original ``_ensure_loop`` failure on the caller's frame.

    Post-fix: ``coro.close()`` runs in the new narrow except,
    suppressing the RuntimeWarning; the original ``_ensure_loop``
    error propagates unchanged.
    """
    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
    # Hand-prime only the attributes _run_sync touches before the
    # _ensure_loop call site. Mirrors the minimal scaffolding used in
    # tests/test_run_sync_*.py.
    import os as _os
    import threading

    conn._creator_thread = threading.get_ident()
    conn._creator_pid = _os.getpid()
    conn._timeout = 5.0
    conn._closed = False
    conn._op_lock = threading.Lock()
    conn._loop_lock = threading.Lock()
    conn._loop = None
    conn._thread = None
    conn._finalizer = None
    conn._async_conn = None
    conn._closed_flag = [False]
    conn._address = "localhost:9001"

    async def some_coro() -> int:
        return 42  # pragma: no cover - never awaited

    # Stub _ensure_loop to raise as OS-resource-exhaustion would.
    def _broken_ensure_loop() -> object:
        raise RuntimeError("synthetic: could not start daemon loop thread")

    monkeypatch.setattr(conn, "_ensure_loop", _broken_ensure_loop)

    coro = some_coro()
    # Sanity: an un-started, un-closed coroutine has a non-None
    # ``cr_frame``. After ``coro.close()`` it becomes None.
    assert coro.cr_frame is not None  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="daemon loop thread"):
        conn._run_sync(coro)

    # Pin: _run_sync must close the coroutine before propagating
    # the _ensure_loop failure. coro.cr_frame is None iff the
    # coroutine has been closed or completed; without the cleanup
    # arm at _ensure_loop's call site the frame would still be set
    # and the next GC pass would emit "coroutine ... was never
    # awaited".
    assert coro.cr_frame is None, (  # type: ignore[attr-defined]
        "_run_sync must call coro.close() when _ensure_loop() raises — "
        "otherwise the unawaited coroutine leaks a RuntimeWarning at "
        "GC time, masking the original _ensure_loop failure"
    )

    # Belt-and-braces: a follow-up GC under strict warnings must
    # not surface a 'coroutine was never awaited' record.
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", RuntimeWarning)
        del coro
        gc.collect()
    never_awaited = [w for w in captured if "was never awaited" in str(w.message)]
    assert not never_awaited, f"unawaited-coroutine warning leaked at GC time: {never_awaited!r}"
