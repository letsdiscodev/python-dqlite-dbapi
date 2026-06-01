"""force_close_transport() must unblock a concurrent in-flight _run_sync caller promptly,
not leave it riding out the full sync_timeout (4×timeout) on a future the stopped loop will
never resolve. (check_same_thread=False; force_close is the documented last-resort path.)
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time

import pytest

import dqlitedbapi
from dqlitedbapi import InterfaceError, OperationalError


@pytest.mark.integration
def test_force_close_unblocks_concurrent_run_sync(cluster_address: str) -> None:
    # sync_timeout = 4 * 5.0 = 20s; pre-fix the concurrent caller wedged the full 20s.
    conn = dqlitedbapi.connect(cluster_address, timeout=5.0, check_same_thread=False)
    try:
        conn.cursor().execute("SELECT 1").fetchone()  # warm up the loop + wire

        result: dict[str, object] = {}
        ready = threading.Event()

        def slow_op() -> None:
            ready.set()
            t0 = time.monotonic()
            try:
                conn._run_sync(asyncio.sleep(30))  # stand-in for a slow in-flight wire op
            except BaseException as e:  # noqa: BLE001
                result["exc"] = e
                result["elapsed"] = time.monotonic() - t0

        t = threading.Thread(target=slow_op)
        t.start()
        ready.wait(timeout=2.0)
        time.sleep(0.3)  # let _run_sync acquire the op_lock and enter the result wait

        conn.force_close_transport()

        t.join(timeout=10.0)
        assert not t.is_alive(), "concurrent _run_sync caller did not unblock"
        assert "exc" in result, "the in-flight op did not raise on force-close"
        elapsed = result["elapsed"]
        assert isinstance(elapsed, float)
        # The whole point: unblocked promptly, NOT after the 20s sync_timeout.
        assert elapsed < 3.0, f"caller wedged {elapsed:.1f}s (sync_timeout was 20s)"
        assert isinstance(result["exc"], (InterfaceError, OperationalError))
    finally:
        with contextlib.suppress(Exception):
            conn.close()
