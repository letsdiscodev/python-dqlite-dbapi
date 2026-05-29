"""Unit tests for ``_is_no_transaction_error``: it mask-compares the SQLite
primary result code so the silent commit()/rollback() swallow only fires on
genuine "no active transaction" replies."""

from dqlitedbapi.connection import _is_no_transaction_error
from dqlitedbapi.exceptions import OperationalError


class TestIsNoTransactionError:
    def test_primary_error_code_matches(self) -> None:
        # SQLITE_ERROR = 1
        exc = OperationalError("cannot commit - no transaction is active", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_misuse_code_does_not_match(self) -> None:
        # SQLITE_MISUSE = 21 is never used by dqlite for transaction-state
        # errors, so a real misuse must surface rather than be swallowed.
        exc = OperationalError("cannot rollback - no transaction is active", code=21)
        assert _is_no_transaction_error(exc) is False

    def test_extended_snapshot_code_matches(self) -> None:
        # SQLITE_ERROR_SNAPSHOT = 769; low byte == 1 (SQLITE_ERROR), so swallow.
        exc = OperationalError("cannot commit - no transaction is active", code=769)
        assert _is_no_transaction_error(exc) is True

    def test_extended_retry_code_matches(self) -> None:
        # SQLITE_ERROR_RETRY = 513; low byte == 1 (SQLITE_ERROR)
        exc = OperationalError("cannot rollback - no transaction is active", code=513)
        assert _is_no_transaction_error(exc) is True

    def test_unrelated_code_does_not_match(self) -> None:
        # SQLITE_CONSTRAINT = 19; must NOT match even if message contains substring.
        exc = OperationalError("no transaction is active but actually constraint", code=19)
        assert _is_no_transaction_error(exc) is False

    def test_code_none_does_not_match_even_with_substring(self) -> None:
        """code=None must NOT swallow: _call_client wraps the errors we want to
        surface (leader-flip/cluster failures) with code=None, and the genuine
        server reply always carries code=1."""
        exc = OperationalError("no transaction is active")
        assert _is_no_transaction_error(exc) is False

    def test_matching_code_but_wrong_message_rejected(self) -> None:
        exc = OperationalError("disk I/O error", code=1)
        assert _is_no_transaction_error(exc) is False

    def test_cannot_commit_with_no_tx_clause_matches(self) -> None:
        # "no transaction is active" is the single canonical substring; the bare
        # "cannot rollback" token was dropped as too permissive.
        exc = OperationalError("cannot commit - no transaction is active", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_bare_cannot_rollback_no_longer_matches(self) -> None:
        exc = OperationalError("cannot rollback because the disk is full", code=1)
        assert _is_no_transaction_error(exc) is False

    def test_cannot_rollback_with_unrelated_code_rejected(self) -> None:
        # The primary-code gate is the real defence; the substring is secondary.
        exc = OperationalError("cannot rollback - constraint failed", code=19)
        assert _is_no_transaction_error(exc) is False

    def test_no_such_savepoint_not_swallowed_unquoted(self) -> None:
        # "no such savepoint" has primary code 1 but lacks the substring, so
        # RELEASE/ROLLBACK TO of an unknown savepoint must surface to the caller.
        exc = OperationalError("no such savepoint: sp1", code=1)
        assert _is_no_transaction_error(exc) is False

    def test_no_such_savepoint_not_swallowed_quoted(self) -> None:
        exc = OperationalError('no such savepoint: "MyPoint"', code=1)
        assert _is_no_transaction_error(exc) is False


class TestIsNoTransactionErrorEdgeCases:
    """Pin the case-insensitive / whitespace / both-substring matrix; the
    recogniser does str(exc).lower() then a substring check."""

    def test_uppercase_message_matches(self) -> None:
        exc = OperationalError("NO TRANSACTION IS ACTIVE", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_mixed_case_message_matches(self) -> None:
        exc = OperationalError("No Transaction Is Active", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_leading_and_trailing_whitespace_matches(self) -> None:
        exc = OperationalError("  no transaction is active  ", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_tab_and_newline_whitespace_matches(self) -> None:
        exc = OperationalError("\tno transaction is active\n", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_both_substrings_concatenated_match(self) -> None:
        exc = OperationalError("cannot rollback - no transaction is active", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_empty_message_with_correct_code_does_not_match(self) -> None:
        # Code-gate alone is not enough; the substring check must still fire.
        exc = OperationalError("", code=1)
        assert _is_no_transaction_error(exc) is False

    def test_substring_inside_unrelated_message_currently_matches(self) -> None:
        # Documents current behaviour: an embedded substring is treated as no-tx;
        # false-positive risk is bounded by the primary-code gate.
        exc = OperationalError("error: cannot find table 'no transaction is active'", code=1)
        assert _is_no_transaction_error(exc) is True

    def test_code_zero_empty_statement_not_swallowed(self) -> None:
        """Pin: code=0 is NOT in _NO_TX_PRIMARY_CODES; upstream emits code 0 for
        empty/comment-only SQL, which must surface as a normal OperationalError
        rather than be swallowed at the commit/rollback boundary."""
        exc = OperationalError("empty statement", code=0)
        assert _is_no_transaction_error(exc) is False
