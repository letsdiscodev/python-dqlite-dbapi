"""Event-loop thread is cleaned up on Connection GC (ISSUE-10).

Previously Connection spawned a daemon thread via _ensure_loop() and
only stopped it on explicit close(). A GC'd connection leaked its
thread forever, which bit long-lived processes (web servers, workers)
accumulating dozens/hundreds of leaked loops over time.

Now _ensure_loop registers a weakref.finalize that stops the loop and
joins the thread when the Connection is GC'd, and emits ResourceWarning
to mirror stdlib sqlite3 conventions.
"""

import gc
import threading
import warnings

from dqlitedbapi.connection import Connection


class TestGCCleanup:
    def test_gc_cleans_up_loop_thread(self) -> None:
        """Connection without explicit close() gets cleaned up on GC."""
        baseline = threading.active_count()
        # Create a connection and force the background loop to exist.
        conn = Connection("localhost:19001", timeout=2.0)
        conn._ensure_loop()
        assert threading.active_count() == baseline + 1

        # GC without close(). Expect ResourceWarning.
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            del conn
            gc.collect()
            # Give the finalizer thread time to join.
            for t in threading.enumerate():
                if t.daemon and t is not threading.current_thread():
                    t.join(timeout=1.0)

        # At least one ResourceWarning about the leak.
        rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
        assert rw, f"expected ResourceWarning; got categories={[w.category for w in captured]}"

    def test_explicit_close_suppresses_resourcewarning(self) -> None:
        """If the user calls close(), finalizer shouldn't emit a warning."""
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            conn = Connection("localhost:19001", timeout=2.0)
            conn._ensure_loop()
            conn.close()
            del conn
            gc.collect()

        # No ResourceWarning since the user did the right thing.
        rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
        assert not rw, f"unexpected ResourceWarning: {[str(w.message) for w in rw]}"
