"""Tests for thread safety enforcement and close() hardening.

The package declares threadsafety=1, meaning connections must not be
shared between threads. Like sqlite3 stdlib, we enforce this with a
thread identity check that raises ProgrammingError.
"""

import threading

from dqlitedbapi import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


class TestThreadIdentityCheck:
    """Test that connections reject cross-thread access."""

    def test_cursor_from_wrong_thread_raises(self) -> None:
        """Creating a cursor from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                conn.cursor()
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)
        assert "thread" in str(error).lower()

    def test_commit_from_wrong_thread_raises(self) -> None:
        """commit() from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                conn.commit()
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_rollback_from_wrong_thread_raises(self) -> None:
        """rollback() from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                conn.rollback()
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_close_from_wrong_thread_raises(self) -> None:
        """close() from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                conn.close()
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_same_thread_works(self) -> None:
        """Operations from the creating thread must work normally."""
        conn = Connection("localhost:9001")
        # These should not raise
        cursor = conn.cursor()
        assert isinstance(cursor, Cursor)
        conn.close()

    def test_cursor_fetchone_from_wrong_thread_raises(self) -> None:
        """Cursor fetch from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        # Pre-populate cursor so fetch doesn't fail for other reasons
        cursor._description = [("id", None, None, None, None, None, None)]
        cursor._rows = [(1,)]

        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                cursor.fetchone()
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_setinputsizes_from_wrong_thread_raises(self) -> None:
        """setinputsizes() from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                cursor.setinputsizes([None])
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_setoutputsize_from_wrong_thread_raises(self) -> None:
        """setoutputsize() from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                cursor.setoutputsize(1)
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_callproc_from_wrong_thread_raises(self) -> None:
        """callproc() from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                cursor.callproc("p")
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_nextset_from_wrong_thread_raises(self) -> None:
        """nextset() from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                cursor.nextset()
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_scroll_from_wrong_thread_raises(self) -> None:
        """scroll() from a different thread must raise ProgrammingError."""
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                cursor.scroll(0)
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)


class TestCloseHardening:
    """Test that close() is idempotent and handles edge cases."""

    def test_double_close_is_safe(self) -> None:
        """Calling close() twice must not raise."""
        conn = Connection("localhost:9001")
        conn.close()
        conn.close()  # Must not raise

    def test_close_sets_closed_immediately(self) -> None:
        """close() must set _closed before doing any cleanup."""
        conn = Connection("localhost:9001")
        conn.close()
        assert conn._closed
