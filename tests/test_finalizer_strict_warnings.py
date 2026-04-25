"""The Connection finalizer's loop/thread teardown must run regardless
of whether the warnings.warn(ResourceWarning, ...) call raises under
strict-warnings mode (``pytest -W error::ResourceWarning``).

Without the try/finally guard, ResourceWarning escalating to a raise
propagated past the narrow ``contextlib.suppress(RuntimeError)`` and
the cleanup steps were skipped — the daemon event-loop thread
lingered with an open socket, ironically *amplifying* the leak the
warning was supposed to surface.
"""

from __future__ import annotations

import asyncio
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

    # Force the warning to escalate to a raise — exactly the
    # ``pytest -W error::ResourceWarning`` regime.
    import contextlib

    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        # closed_flag[0] is False so the finalizer emits the warning.
        # Before the fix this raise propagated past the cleanup block;
        # after the fix the try/finally guarantees cleanup runs. The
        # weakref.finalize machinery would normally consume the raise
        # via sys.unraisablehook; we let it surface and continue here
        # — the contract under test is "cleanup ran regardless."
        with contextlib.suppress(ResourceWarning):
            _cleanup_loop_thread(loop, t, [False], "localhost:9001")

    assert stopped.wait(timeout=2), "loop thread did not terminate"
    assert loop.is_closed(), "event loop was not closed"
