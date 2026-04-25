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

    def test_code_none_does_not_match_even_with_substring(self) -> None:
        """A code-None OperationalError must NOT swallow even when the
        message contains the magic substring.

        The dbapi's ``_call_client`` wraps DqliteConnectionError /
        ClusterError / ProtocolError / DataError with code=None.
        Those classes are precisely the errors we want to surface
        (leader-flip disconnects, cluster failures), never silently
        swallow. The integration test ``test_no_transaction_error_wording``
        proves the genuine server reply always carries code=1, so the
        whitelist is exhaustive on its own — the substring fallback is
        only valid alongside a real SQLite code.
        """
        exc = OperationalError("no transaction is active")
        assert _is_no_transaction_error(exc) is False

    def test_matching_code_but_wrong_message_rejected(self) -> None:
        exc = OperationalError("disk I/O error", code=1)
        assert _is_no_transaction_error(exc) is False

    def test_cannot_rollback_substring_matches(self) -> None:
        # The client-layer sibling helper ``_is_no_tx_rollback_error``
        # accepts both ``"no transaction is active"`` and
        # ``"cannot rollback"`` substrings. Mirror the dbapi recogniser
        # so a future server wording that drops the trailing
        # ``"no transaction is active"`` clause does not produce a
        # silent layer divergence (dbapi raising while client treats
        # it as benign).
        exc = OperationalError("cannot rollback", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_cannot_rollback_with_unrelated_code_rejected(self) -> None:
        # Code-gate (primary code 1) is the real defence — the
        # substring is secondary. A constraint-failed message that
        # happens to contain the magic substring must NOT be
        # swallowed.
        exc = OperationalError("cannot rollback - constraint failed", code=19)
        assert _is_no_transaction_error(exc) is False

    def test_no_such_savepoint_not_swallowed_unquoted(self) -> None:
        # SQLite's "no such savepoint: <name>" error has primary code
        # 1 but its wording does NOT contain "no transaction is active"
        # or "cannot rollback". The substring guard must therefore
        # reject — RELEASE / ROLLBACK TO of an unknown savepoint must
        # surface to the caller, never be silently swallowed by
        # commit() / rollback().
        exc = OperationalError("no such savepoint: sp1", code=1)
        assert _is_no_transaction_error(exc) is False

    def test_no_such_savepoint_not_swallowed_quoted(self) -> None:
        exc = OperationalError('no such savepoint: "MyPoint"', code=1)
        assert _is_no_transaction_error(exc) is False
