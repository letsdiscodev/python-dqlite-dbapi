"""Connection finalizer's loop/thread teardown runs even when the ResourceWarning
escalates to a raise under ``pytest -W error::ResourceWarning``."""

from __future__ import annotations

import asyncio
import os
import threading
import warnings

from dqlitedbapi.connection import _cleanup_loop_thread


def test_cleanup_runs_even_when_resource_warning_escalates() -> None:
    loop = asyncio.new_event_loop()
    started = threading.Event()
    stopped = threading.Event()

    def _runner() -> None:
        asyncio.set_event_loop(loop)
        started.set()
        loop.run_forever()
        stopped.set()

    t = threading.Thread(target=_runner, daemon=True, name="dqlite-test-loop")
    t.start()
    assert started.wait(timeout=2)

    import contextlib

    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        # closed_flag[0] is False so the finalizer emits the warning; we let it surface.
        with contextlib.suppress(ResourceWarning):
            _cleanup_loop_thread(loop, t, [False], "localhost:9001", os.getpid())

    assert stopped.wait(timeout=2), "loop thread did not terminate"
    assert loop.is_closed(), "event loop was not closed"
