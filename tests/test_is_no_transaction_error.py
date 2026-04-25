"""Unit tests for ``_is_no_transaction_error``.

The helper mask-compares the SQLite primary result code (low byte of
the extended code) to pin the silent ``commit()``/``rollback()`` swallow
to genuine "no active transaction" replies. This mirrors the mask
already performed by ``_classify_operational`` in cursor.py.
"""

from dqlitedbapi.connection import _is_no_transaction_error
from dqlitedbapi.exceptions import OperationalError


class TestIsNoTransactionError:
    def test_primary_error_code_matches(self) -> None:
        # SQLITE_ERROR = 1
        exc = OperationalError("cannot commit - no transaction is active", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_misuse_code_does_not_match(self) -> None:
        # SQLITE_MISUSE = 21 — never used by the dqlite server for
        # transaction-state errors (verified upstream:
        # ``dqlite-upstream/src/vfs.c::vfsFileControlPersistWal`` is the
        # only MISUSE site, and it's an unrelated VFS file-control
        # path). A real misuse must surface as a real error, not a
        # silent commit/rollback no-op.
        exc = OperationalError("cannot rollback - no transaction is active", code=21)
        assert _is_no_transaction_error(exc) is False

    def test_extended_snapshot_code_matches(self) -> None:
        # SQLITE_ERROR_SNAPSHOT = 769 = 3 << 8 | 1
        # Low byte == 1 (SQLITE_ERROR), so the swallow must apply.
        exc = OperationalError("cannot commit - no transaction is active", code=769)
        assert _is_no_transaction_error(exc) is True

    def test_extended_retry_code_matches(self) -> None:
        # SQLITE_ERROR_RETRY = 513 = 2 << 8 | 1
        exc = OperationalError("cannot rollback - no transaction is active", code=513)
        assert _is_no_transaction_error(exc) is True

    def test_unrelated_code_does_not_match(self) -> None:
        # SQLITE_CONSTRAINT = 19; must NOT match even if message contains substring.
        exc = OperationalError("no transaction is active but actually constraint", code=19)
        assert _is_no_transaction_error(exc) is False

    def test_code_none_falls_through_to_substring(self) -> None:
        exc = OperationalError("no transaction is active")
        assert _is_no_transaction_error(exc) is True

    def test_matching_code_but_wrong_message_rejected(self) -> None:
        exc = OperationalError("disk I/O error", code=1)
        assert _is_no_transaction_error(exc) is False
