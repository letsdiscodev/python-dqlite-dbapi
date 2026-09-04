"""The event-loop thread is cleaned up on Connection GC: _ensure_loop
registers a weakref.finalize that stops the loop, joins the thread, and
emits ResourceWarning (mirroring stdlib sqlite3).
"""

import gc
import threading
import warnings

from dqlitedbapi.connection import Connection


class TestGCCleanup:
    def test_gc_cleans_up_loop_thread(self) -> None:
        """Connection without explicit close() gets cleaned up on GC."""
        baseline = threading.active_count()
        conn = Connection("localhost:19001", timeout=2.0)
        conn._ensure_loop()
        assert threading.active_count() == baseline + 1

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            del conn
            gc.collect()
            for t in threading.enumerate():
                if t.daemon and t is not threading.current_thread():
                    t.join(timeout=1.0)

        rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
        assert rw, f"expected ResourceWarning; got categories={[w.category for w in captured]}"

    def test_explicit_close_suppresses_resourcewarning(self) -> None:
        """If the user calls close(), the finalizer emits no warning."""
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            conn = Connection("localhost:19001", timeout=2.0)
            conn._ensure_loop()
            conn.close()
            del conn
            gc.collect()

        rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
        assert not rw, f"unexpected ResourceWarning: {[str(w.message) for w in rw]}"
