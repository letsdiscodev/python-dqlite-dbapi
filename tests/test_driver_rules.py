"""Driver-level rules that need no cluster: statement classification, BEGIN rewriting,
the busy-retry curve, error translation, and the sync surface's thread confinement."""

from __future__ import annotations

import threading

import pytest

import dqliteclient.exceptions as client_exc
import dqlitedbapi
from dqlitedbapi import _busy, _sql
from dqlitedbapi.exceptions import (
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    translate,
)
from dqlitewire import SQLITE_BUSY, SQLITE_CONSTRAINT


class TestClassify:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  -- lead\n  select 1",
            "(SELECT 1)",
            "VALUES (1)",
            "PRAGMA table_info(t)",
            "EXPLAIN SELECT 1",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT x FROM c",
            "INSERT INTO t VALUES (1) RETURNING id",
            "DELETE FROM t\nRETURNING *",
        ],
    )
    def test_row_returning(self, sql: str) -> None:
        assert _sql.classify(sql).returns_rows

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t VALUES ('RETURNING')",
            "UPDATE t SET name = 'select' WHERE id = 1",
            "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c",
            "CREATE TABLE t (id INTEGER)",
            "BEGIN",
        ],
    )
    def test_not_row_returning(self, sql: str) -> None:
        assert not _sql.classify(sql).returns_rows

    def test_dml_flags(self) -> None:
        stmt = _sql.classify("WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c")
        assert stmt.is_dml and stmt.is_insert and not stmt.is_pragma
        assert _sql.classify("REPLACE INTO t VALUES (1)").is_insert
        assert _sql.classify("UPDATE t SET a = 1").is_dml
        assert not _sql.classify("UPDATE t SET a = 1").is_insert
        assert _sql.classify("PRAGMA foreign_keys = ON").is_pragma
        assert _sql.classify("END").is_commit
        assert _sql.classify("/* x */ commit").is_commit
        assert _sql.classify("SAVEPOINT sp").is_tx_control


class TestValidateOperation:
    @pytest.mark.parametrize("sql", ["", "   ", "-- only a comment", "/* c */", ";", " ; -- x\n;"])
    def test_empty(self, sql: str) -> None:
        with pytest.raises(ProgrammingError, match="empty statement"):
            _sql.validate_operation(sql)

    def test_multi_statement(self) -> None:
        with pytest.raises(ProgrammingError, match="one statement"):
            _sql.validate_operation("SELECT 1; SELECT 2")

    def test_trailing_semicolon_and_comment_allowed(self) -> None:
        assert _sql.validate_operation("SELECT 1; -- done") == "SELECT 1; -- done"

    def test_trigger_body_is_one_statement(self) -> None:
        sql = "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET a = 1; DELETE FROM u; END"
        assert _sql.validate_operation(sql) == sql

    def test_nul_rejected(self) -> None:
        with pytest.raises(ProgrammingError, match="null character"):
            _sql.validate_operation("SELECT '\x00'")

    def test_non_str_rejected(self) -> None:
        with pytest.raises(ProgrammingError, match="str"):
            _sql.validate_operation(b"SELECT 1")


class TestPlaceholders:
    def test_literal_question_marks_ignored(self) -> None:
        assert _sql.placeholder_count("SELECT '?', \"?\", ? -- ?\n, ?") == 2

    def test_count_mismatch(self) -> None:
        with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
            _sql.check_placeholder_count("SELECT ?, ?", (1,))


class TestRewriteBegin:
    @pytest.mark.parametrize(
        ("mode", "expected"),
        [
            ("immediate", "BEGIN IMMEDIATE"),
            ("exclusive", "BEGIN EXCLUSIVE"),
            ("deferred", "begin transaction;"),
            ("read_only", "begin transaction;"),
        ],
    )
    def test_bare_begin(self, mode: str, expected: str) -> None:
        assert _sql.rewrite_begin("begin transaction;", mode) == expected

    @pytest.mark.parametrize("sql", ["BEGIN DEFERRED", "BEGIN IMMEDIATE", "BEGIN EXCLUSIVE"])
    def test_explicit_qualifier_untouched(self, sql: str) -> None:
        assert _sql.rewrite_begin(sql, "immediate") == sql


class TestBusyTimeoutPragma:
    def test_getter(self) -> None:
        assert _sql.busy_timeout_pragma("PRAGMA busy_timeout") == (True, None)

    @pytest.mark.parametrize("sql", ["PRAGMA busy_timeout = 250", "pragma busy_timeout(250);"])
    def test_setter(self, sql: str) -> None:
        assert _sql.busy_timeout_pragma(sql) == (True, 250)

    @pytest.mark.parametrize("value", ["-1", "0", str(2**31)])
    def test_out_of_range_collapses_to_zero(self, value: str) -> None:
        assert _sql.busy_timeout_pragma(f"PRAGMA busy_timeout = {value}") == (True, 0)

    def test_other_pragma(self) -> None:
        assert _sql.busy_timeout_pragma("PRAGMA foreign_keys") == (False, None)


class TestBusyCurve:
    def test_follows_sqlite_delays_until_budget(self) -> None:
        delays = []
        attempt = 0
        while (delay := _busy.next_delay_ms(attempt, 100)) is not None:
            delays.append(delay)
            attempt += 1
        assert delays == [1, 2, 5, 10, 15, 20, 25, 22]
        assert sum(delays) == 100

    def test_zero_budget_never_retries(self) -> None:
        assert _busy.next_delay_ms(0, 0) is None

    def test_is_busy(self) -> None:
        assert _busy.is_busy(OperationalError("busy", code=SQLITE_BUSY))
        assert not _busy.is_busy(OperationalError("other", code=1))
        assert not _busy.is_busy(InterfaceError("x", code=SQLITE_BUSY))


class TestTranslate:
    def test_constraint_becomes_integrity_error(self) -> None:
        exc = client_exc.OperationalError(
            "dup", SQLITE_CONSTRAINT | (6 << 8), raw_message="dup raw"
        )
        mapped = translate(exc)
        assert isinstance(mapped, IntegrityError)
        assert mapped.code == exc.code and mapped.raw_message == "dup raw"

    def test_connection_error_keeps_code(self) -> None:
        mapped = translate(client_exc.DqliteConnectionError("gone", code=10250))
        assert type(mapped) is OperationalError and mapped.code == 10250

    def test_policy_rejection_is_interface_error_with_prefix(self) -> None:
        mapped = translate(client_exc.ClusterPolicyError("denied"))
        assert isinstance(mapped, InterfaceError)
        assert str(mapped).startswith(dqlitedbapi.CLUSTER_POLICY_REJECTION_PREFIX)

    def test_data_error(self) -> None:
        assert isinstance(translate(client_exc.DataError("bad")), DataError)

    def test_foreign_exception_untouched(self) -> None:
        assert translate(KeyError("x")) is None


class TestSyncThreadConfinement:
    def test_other_thread_rejected_by_default(self) -> None:
        conn = dqlitedbapi.connect("127.0.0.1:1")
        caught: list[BaseException] = []

        def worker() -> None:
            try:
                conn.cursor()
            except BaseException as exc:  # noqa: BLE001
                caught.append(exc)

        t = threading.Thread(target=worker)
        t.start()
        t.join()
        conn.close()
        assert len(caught) == 1 and isinstance(caught[0], ProgrammingError)

    def test_check_same_thread_false_allows_other_threads(self) -> None:
        conn = dqlitedbapi.connect("127.0.0.1:1", check_same_thread=False)
        result: list[object] = []

        def worker() -> None:
            result.append(conn.cursor())

        t = threading.Thread(target=worker)
        t.start()
        t.join()
        conn.close()
        assert len(result) == 1

    def test_force_close_from_other_thread_is_allowed(self) -> None:
        conn = dqlitedbapi.connect("127.0.0.1:1")
        t = threading.Thread(target=conn.force_close_transport)
        t.start()
        t.join()
        assert conn.closed
        with pytest.raises(InterfaceError):
            conn.cursor()


class TestConstructorValidation:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"timeout": 0},
            {"timeout": True},
            {"close_timeout": -1.0},
            {"busy_timeout": -1},
            {"busy_timeout": "5"},
            {"max_total_rows": 0},
            {"session_mode": "fast"},
            {"database": " padded"},
            {"check_same_thread": 1},
        ],
    )
    def test_bad_argument_is_programming_error(self, kwargs: dict[str, object]) -> None:
        with pytest.raises(ProgrammingError):
            dqlitedbapi.connect("127.0.0.1:1", **kwargs)  # type: ignore[arg-type]

    def test_unknown_stdlib_kwarg_is_not_supported(self) -> None:
        with pytest.raises(dqlitedbapi.NotSupportedError, match="detect_types"):
            dqlitedbapi.connect("127.0.0.1:1", detect_types=1)
