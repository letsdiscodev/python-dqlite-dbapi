"""Thread-safety DEFAULT enforcement and close() hardening. ``threadsafety=2`` is the
capability ceiling, but the default ``check_same_thread=True`` enforces strict per-thread use;
the relaxation is tested in ``test_check_same_thread_kwarg.py``."""

import threading

from dqlitedbapi import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


class TestThreadIdentityCheck:
    def test_cursor_from_wrong_thread_raises(self) -> None:
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
        cursor = conn.cursor()
        assert isinstance(cursor, Cursor)
        conn.close()

    def test_cursor_fetchone_from_wrong_thread_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        # Pre-populate so fetch doesn't fail for other reasons.
        cursor._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
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

    def test_row_factory_setter_from_wrong_thread_raises(self) -> None:
        """row_factory setter from a foreign thread must raise: cross-thread mutation could
        otherwise override the row_factory used by cursors on the creator thread."""
        conn = Connection("localhost:9001")
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                conn.row_factory = lambda c, r: r
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_autocommit_setter_from_wrong_thread_raises(self) -> None:
        """autocommit setter from a foreign thread must raise even on the no-op accept-path."""
        conn = Connection("localhost:9001")
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                conn.autocommit = True  # no-op accept-path; must still raise
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_isolation_level_setter_from_wrong_thread_raises(self) -> None:
        """isolation_level setter from a foreign thread must raise even on the None accept-path."""
        conn = Connection("localhost:9001")
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                conn.isolation_level = None  # no-op accept-path; must still raise
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_text_factory_setter_from_wrong_thread_raises(self) -> None:
        """text_factory setter from a foreign thread must raise even on the str accept-path."""
        conn = Connection("localhost:9001")
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                conn.text_factory = str  # no-op accept-path; must still raise
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_cursor_arraysize_setter_from_wrong_thread_raises(self) -> None:
        """Cursor.arraysize setter from a foreign thread must raise: a mid-batch change would
        silently alter the creator thread's next ``fetchmany`` size."""
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                cursor.arraysize = 1
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_cursor_row_factory_setter_from_wrong_thread_raises(self) -> None:
        """Cursor.row_factory setter from a foreign thread must raise: it could otherwise swap
        the creator thread's per-row materialisation hook mid-fetch."""
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        error: Exception | None = None

        def wrong_thread() -> None:
            nonlocal error
            try:
                cursor.row_factory = lambda c, r: dict(
                    zip([d[0] for d in c.description], r, strict=False)
                )
            except Exception as e:
                error = e

        t = threading.Thread(target=wrong_thread)
        t.start()
        t.join()

        assert isinstance(error, ProgrammingError)

    def test_setinputsizes_from_wrong_thread_raises(self) -> None:
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
    def test_double_close_is_safe(self) -> None:
        conn = Connection("localhost:9001")
        conn.close()
        conn.close()

    def test_close_sets_closed_immediately(self) -> None:
        """close() must set _closed before doing any cleanup."""
        conn = Connection("localhost:9001")
        conn.close()
        assert conn._closed
