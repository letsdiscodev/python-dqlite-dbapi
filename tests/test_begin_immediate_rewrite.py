"""Unit tests for the ``BEGIN`` → ``BEGIN IMMEDIATE`` rewrite at the
dbapi cursor layer.

dqlite-server's VFS recommends ``BEGIN IMMEDIATE`` for write-bearing
transactions so the writer-lock is held from BEGIN onwards, eliminating
the ``SQLITE_BUSY_SNAPSHOT (517)`` race for the SELECT-then-INSERT
pattern. The rewrite is on by default; off-switch via
``connect(..., begin_immediate=False)`` or ``DQLITE_BEGIN_IMMEDIATE=0``.

These tests cover only the SQL-string rewriter — the integration test
in ``sqlalchemy-dqlite/tests/integration/test_concurrent_writers_begin_immediate.py``
covers the end-to-end "8 concurrent writers all commit" contract.
"""

from __future__ import annotations

import pytest

from dqlitedbapi._pragma_intercept import (
    begin_immediate_default_from_env,
    try_rewrite_begin_to_immediate,
)


class TestRewrittenForms:
    """Plain BEGIN family — rewritten to ``BEGIN IMMEDIATE``."""

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
        assert try_rewrite_begin_to_immediate(stmt, enabled=True) == "BEGIN IMMEDIATE"


class TestPassThroughForms:
    """Explicit-intent and non-BEGIN statements pass through unchanged
    (the rewriter returns ``None`` so the caller leaves the SQL as-is).

    ``BEGIN DEFERRED`` is on this list: it is the explicit per-session
    opt-out signal emitted by the SA dialect when the user sets
    ``execution_options(dqlite_begin_mode="deferred")``. Plain bare
    ``BEGIN`` is the only ambiguous shape the rewrite touches.
    """

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
        assert try_rewrite_begin_to_immediate(stmt, enabled=True) is None

    def test_multi_statement_not_rewritten(self) -> None:
        # A trailing keyword after the BEGIN means this is not a bare
        # BEGIN — leave it to the wire's multi-statement classifier.
        assert try_rewrite_begin_to_immediate("BEGIN; SELECT 1", enabled=True) is None


class TestOffSwitch:
    """``enabled=False`` skips the rewrite entirely."""

    def test_disabled_passes_begin_through(self) -> None:
        assert try_rewrite_begin_to_immediate("BEGIN", enabled=False) is None

    def test_disabled_passes_begin_transaction_through(self) -> None:
        assert try_rewrite_begin_to_immediate("BEGIN TRANSACTION", enabled=False) is None


class TestNonStringInput:
    """Defensive: a non-str ``statement`` (test fixtures pass bytes
    sometimes) returns None rather than raising."""

    def test_bytes_input(self) -> None:
        assert try_rewrite_begin_to_immediate(b"BEGIN", enabled=True) is None  # type: ignore[arg-type]

    def test_none_input(self) -> None:
        assert try_rewrite_begin_to_immediate(None, enabled=True) is None  # type: ignore[arg-type]


class TestEnvVarDefault:
    """``DQLITE_BEGIN_IMMEDIATE`` env-var controls the default for
    callers who don't pass an explicit kwarg."""

    @pytest.mark.parametrize("value", ["0", "false", "FALSE", "Off", "no"])
    def test_disabled_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("DQLITE_BEGIN_IMMEDIATE", value)
        assert begin_immediate_default_from_env() is False

    @pytest.mark.parametrize("value", ["1", "true", "on", "yes", "anything", ""])
    def test_enabled_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("DQLITE_BEGIN_IMMEDIATE", value)
        assert begin_immediate_default_from_env() is True

    def test_unset_honours_import_time_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # When the env var is unset at call time, fall back to the
        # module-level cache. The cache was populated at import time
        # before this test could touch the env, so its value depends
        # on the test runner's env at module import. Just assert the
        # result is a bool.
        monkeypatch.delenv("DQLITE_BEGIN_IMMEDIATE", raising=False)
        assert isinstance(begin_immediate_default_from_env(), bool)
