"""Pin: ``_run_sync`` closes the coroutine if ``_ensure_loop()`` raises before scheduling."""

from __future__ import annotations

import gc
import warnings

import pytest

import dqlitedbapi


def test_run_sync_closes_coro_when_ensure_loop_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_ensure_loop()`` raising must still close ``coro`` so no RuntimeWarning leaks at GC."""
    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
    # Prime only the attributes _run_sync touches before the _ensure_loop call site.
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

    def _broken_ensure_loop() -> object:
        raise RuntimeError("synthetic: could not start daemon loop thread")

    monkeypatch.setattr(conn, "_ensure_loop", _broken_ensure_loop)

    coro = some_coro()
    # cr_frame is non-None until the coroutine is closed/completed.
    assert coro.cr_frame is not None  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="daemon loop thread"):
        conn._run_sync(coro)

    assert coro.cr_frame is None, (  # type: ignore[attr-defined]
        "_run_sync must call coro.close() when _ensure_loop() raises — "
        "otherwise the unawaited coroutine leaks a RuntimeWarning at "
        "GC time, masking the original _ensure_loop failure"
    )

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", RuntimeWarning)
        del coro
        gc.collect()
    never_awaited = [w for w in captured if "was never awaited" in str(w.message)]
    assert not never_awaited, f"unawaited-coroutine warning leaked at GC time: {never_awaited!r}"
