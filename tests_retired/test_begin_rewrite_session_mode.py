"""Unit tests for the ``BEGIN`` → ``BEGIN IMMEDIATE`` rewrite (fires only when
session_mode is "immediate") that avoids the SQLITE_BUSY_SNAPSHOT (517) race."""

from __future__ import annotations

import pytest

from dqlitedbapi._pragma_intercept import (
    session_mode_default_from_env,
    try_rewrite_begin_to_immediate,
    validate_session_mode,
)


class TestRewrittenForms:
    """Plain BEGIN family, rewritten to BEGIN IMMEDIATE when session_mode is immediate."""

    @pytest.mark.parametrize(
        "stmt",
        [
            "BEGIN",
            "BEGIN;",
            "begin",
            "begin;",
            "BEGIN TRANSACTION",
            "BEGIN TRANSACTION;",
            "  begin  ;  ",
            "\tBEGIN\n",
        ],
    )
    def test_plain_begin_rewrites_to_immediate(self, stmt: str) -> None:
        assert try_rewrite_begin_to_immediate(stmt, session_mode="immediate") == "BEGIN IMMEDIATE"


class TestPassThroughForms:
    """Explicit-intent and non-BEGIN statements pass through (rewriter returns None).
    Plain bare BEGIN is the only ambiguous shape the rewrite touches."""

    @pytest.mark.parametrize(
        "stmt",
        [
            "BEGIN IMMEDIATE",
            "BEGIN IMMEDIATE;",
            "begin immediate",
            "BEGIN EXCLUSIVE",
            "BEGIN EXCLUSIVE;",
            "begin exclusive transaction",
            "BEGIN DEFERRED",
            "BEGIN DEFERRED;",
            "begin deferred",
            "BEGIN DEFERRED TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT a",
            "SELECT 1",
            "INSERT INTO t VALUES (1)",
            "PRAGMA busy_timeout = 5000",
        ],
    )
    def test_passthrough(self, stmt: str) -> None:
        assert try_rewrite_begin_to_immediate(stmt, session_mode="immediate") is None

    def test_multi_statement_not_rewritten(self) -> None:
        # Trailing keyword after BEGIN: not bare, left to the wire's classifier.
        assert try_rewrite_begin_to_immediate("BEGIN; SELECT 1", session_mode="immediate") is None


class TestNonImmediateModes:
    """Non-immediate modes skip the rewrite; bare BEGIN passes through (SQLite treats
    it as DEFERRED)."""

    @pytest.mark.parametrize("mode", ["deferred", "exclusive", "read_only"])
    def test_non_immediate_modes_pass_bare_begin_through(self, mode: str) -> None:
        assert try_rewrite_begin_to_immediate("BEGIN", session_mode=mode) is None

    @pytest.mark.parametrize("mode", ["deferred", "exclusive", "read_only"])
    def test_non_immediate_modes_pass_begin_transaction_through(self, mode: str) -> None:
        assert try_rewrite_begin_to_immediate("BEGIN TRANSACTION", session_mode=mode) is None


class TestNonStringInput:
    """Defensive: a non-str statement returns None rather than raising."""

    def test_bytes_input(self) -> None:
        assert (
            try_rewrite_begin_to_immediate(b"BEGIN", session_mode="immediate")  # type: ignore[arg-type]
            is None
        )

    def test_none_input(self) -> None:
        assert (
            try_rewrite_begin_to_immediate(None, session_mode="immediate")  # type: ignore[arg-type]
            is None
        )


class TestEnvVarDefault:
    """DQLITE_SESSION_MODE env-var sets the default when no session_mode kwarg given."""

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            ("immediate", "immediate"),
            ("IMMEDIATE", "immediate"),
            ("deferred", "deferred"),
            ("Deferred", "deferred"),
            ("exclusive", "exclusive"),
            ("read_only", "read_only"),
            ("READ_ONLY", "read_only"),
        ],
    )
    def test_known_values(
        self,
        monkeypatch: pytest.MonkeyPatch,
        env_value: str,
        expected: str,
    ) -> None:
        monkeypatch.setenv("DQLITE_SESSION_MODE", env_value)
        assert session_mode_default_from_env() == expected

    @pytest.mark.parametrize("value", ["", "  "])
    def test_empty_value_falls_back_to_immediate(
        self,
        monkeypatch: pytest.MonkeyPatch,
        value: str,
    ) -> None:
        monkeypatch.setenv("DQLITE_SESSION_MODE", value)
        assert session_mode_default_from_env() == "immediate"

    @pytest.mark.parametrize("value", ["read-only", "RO", "1", "yes", "writer"])
    def test_invalid_value_raises_value_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        value: str,
    ) -> None:
        monkeypatch.setenv("DQLITE_SESSION_MODE", value)
        with pytest.raises(ValueError, match="DQLITE_SESSION_MODE"):
            session_mode_default_from_env()

    def test_unset_falls_back_to_known_mode(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("DQLITE_SESSION_MODE", raising=False)
        # Falls back to the import-time read; assert it's at least a recognised mode.
        result = session_mode_default_from_env()
        assert result in {"immediate", "deferred", "exclusive", "read_only"}


class TestValidateSessionMode:
    """validate_session_mode coerces user-supplied values to canonical lowercase."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("immediate", "immediate"),
            ("IMMEDIATE", "immediate"),
            ("Immediate", "immediate"),
            ("deferred", "deferred"),
            ("exclusive", "exclusive"),
            ("read_only", "read_only"),
            ("READ_ONLY", "read_only"),
        ],
    )
    def test_known_values_normalised(self, raw: str, expected: str) -> None:
        assert validate_session_mode(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        ["read-only", "RO", "writer", "rw", "1", "", "  "],
    )
    def test_invalid_values_raise(self, raw: str) -> None:
        with pytest.raises(ValueError, match="Invalid session_mode"):
            validate_session_mode(raw)

    @pytest.mark.parametrize("raw", [None, True, 1, 1.0, ("immediate",)])
    def test_non_string_input_raises(self, raw: object) -> None:
        with pytest.raises(ValueError, match="must be a str"):
            validate_session_mode(raw)
