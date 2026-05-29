"""``_is_multi_statement`` must treat a single ``CREATE TRIGGER ... BEGIN
... END`` as ONE statement (its body's inner ``;`` are not statement
boundaries), matching stdlib ``sqlite3`` and the client layer's
trigger-aware splitter — while still rejecting genuine multi-statement
batches and preserving the tuned empty-statement / stray-``;`` behaviour.
"""

from __future__ import annotations

import uuid

from dqlitedbapi import Connection
from dqlitedbapi.cursor import _is_multi_statement


def test_single_trigger_with_body_is_not_multi_statement() -> None:
    for sql in (
        "CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO l VALUES(1); END",
        "CREATE TEMP TRIGGER trg AFTER INSERT ON t "
        "BEGIN INSERT INTO l VALUES(1); UPDATE x SET a=1; END",
        "CREATE TEMPORARY TRIGGER trg BEFORE DELETE ON t BEGIN SELECT RAISE(ABORT, 'no'); END",
        "create trigger trg after insert on t begin select raise(abort,'x'); end",
        "  -- comment\n  CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO l VALUES(1); END",
    ):
        assert _is_multi_statement(sql) is False, sql


def test_trigger_followed_by_second_statement_is_multi_statement() -> None:
    sql = "CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO l VALUES(1); END; DROP TABLE z"
    assert _is_multi_statement(sql) is True


def test_genuine_batches_still_rejected() -> None:
    for sql in (
        "INSERT INTO t VALUES(1); INSERT INTO t VALUES(2)",
        "SELECT 1; SELECT 2",
        "BEGIN; INSERT INTO t VALUES(1); COMMIT;",
    ):
        assert _is_multi_statement(sql) is True, sql


def test_non_trigger_edge_cases_unchanged() -> None:
    # The surgical trigger carve-out must NOT alter the tuned flat-scan
    # behaviour for non-trigger SQL (empty statements / stray ';').
    assert _is_multi_statement("SELECT 1") is False
    assert _is_multi_statement("SELECT 1;") is False
    assert _is_multi_statement(";") is False
    assert _is_multi_statement(";;") is True
    assert _is_multi_statement("SELECT 1;;") is True
    assert _is_multi_statement(";SELECT 1") is True


def test_create_trigger_executes_end_to_end_and_fires() -> None:
    suffix = uuid.uuid4().hex[:8]
    base = f"trg_{suffix}"
    conn = Connection("localhost:9001", database=f"trigger_e2e_{suffix}")
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {base}_t (id INTEGER PRIMARY KEY, v TEXT)")
        cur.execute(f"CREATE TABLE {base}_log (msg TEXT)")
        # This previously raised ProgrammingError("You can only execute one
        # statement at a time.") due to the trigger body's inner ';'.
        cur.execute(
            f"CREATE TRIGGER {base}_trg AFTER INSERT ON {base}_t "
            f"BEGIN INSERT INTO {base}_log(msg) VALUES('fired'); END"
        )
        cur.execute(f"INSERT INTO {base}_t (v) VALUES ('x')")
        conn.commit()
        cur.execute(f"SELECT msg FROM {base}_log")
        assert cur.fetchall() == [("fired",)]
    finally:
        conn.close()
