"""Driver behaviour exercised against a scripted stand-in for the client's DqliteConnection.

The stub is attached where the real client would be, so these tests need no cluster and
can script server replies (result codes, row shapes) that are hard to provoke live.
"""

from __future__ import annotations

import datetime
import os
from collections.abc import Iterator, Sequence
from typing import Any

import pytest

import dqliteclient.exceptions as client_exc
import dqlitedbapi
from dqlitedbapi import UNKNOWN, Row
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import (
    AMBIGUOUS_COMMIT_CODES,
    AmbiguousCommitError,
    DataError,
    IntegrityError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from dqlitewire import LEADER_ERROR_CODES, SQLITE_BUSY, SQLITE_CONSTRAINT, SQLITE_ERROR, ValueType

NOT_LEADER = min(LEADER_ERROR_CODES - AMBIGUOUS_COMMIT_CODES)
LEADERSHIP_LOST = min(AMBIGUOUS_COMMIT_CODES)

INT, TEXT, NULL = int(ValueType.INTEGER), int(ValueType.TEXT), int(ValueType.NULL)
ISO, UNIX = int(ValueType.ISO8601), int(ValueType.UNIXTIME)

QueryReply = tuple[list[str], list[int], list[list[int]], list[list[Any]]]


class StubClient:
    """Records statements and returns scripted replies."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, Sequence[Any] | None]] = []
        self.in_transaction = False
        self.is_connected = True
        self.errors: list[BaseException] = []
        self.exec_reply: tuple[int, int] = (0, 0)
        self.query_reply: QueryReply = ([], [], [], [])
        self.disconnect_on_error = False

    def _record(self, sql: str, params: Sequence[Any] | None) -> None:
        self.statements.append((sql, params))
        if self.errors:
            error = self.errors.pop(0)
            if self.disconnect_on_error:
                self.is_connected = False
            raise error
        verb = sql.split()[0].upper()
        if verb == "BEGIN":
            self.in_transaction = True
        elif verb in ("COMMIT", "END", "ROLLBACK"):
            self.in_transaction = False

    async def execute(self, sql: str, params: Sequence[Any] | None = None) -> tuple[int, int]:
        self._record(sql, params)
        return self.exec_reply

    async def query_raw_typed(self, sql: str, params: Sequence[Any] | None = None) -> QueryReply:
        self._record(sql, params)
        return self.query_reply

    async def close(self) -> None:
        self.is_connected = False

    def terminate(self) -> None:
        self.is_connected = False

    @property
    def sql(self) -> list[str]:
        return [s for s, _ in self.statements]


def server_error(code: int, message: str = "server says no") -> client_exc.OperationalError:
    return client_exc.OperationalError(message, code)


@pytest.fixture
def stub() -> StubClient:
    return StubClient()


@pytest.fixture
def aconn(stub: StubClient) -> AsyncConnection:
    conn = AsyncConnection("localhost:19001", busy_timeout=0.05)
    conn._client = stub  # type: ignore[assignment]
    return conn


@pytest.fixture
def conn(stub: StubClient) -> Iterator[dqlitedbapi.Connection]:
    connection = dqlitedbapi.connect("localhost:19001", busy_timeout=0.05)
    connection._async._client = stub  # type: ignore[assignment]
    yield connection
    connection.close()


class TestQueryResults:
    async def test_description_and_rows(self, aconn: AsyncConnection, stub: StubClient) -> None:
        stub.query_reply = (
            ["id", "name"],
            [INT, TEXT],
            [[INT, TEXT], [INT, TEXT]],
            [[1, "a"], [2, "b"]],
        )
        cur = await aconn.execute("SELECT id, name FROM t")
        assert cur.description == (
            ("id", INT, None, None, None, None, None),
            ("name", TEXT, None, None, None, None, None),
        )
        assert cur.rowcount == 2
        assert await cur.fetchall() == [(1, "a"), (2, "b")]

    async def test_null_first_row_takes_type_from_later_row(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.query_reply = (["v"], [NULL], [[NULL], [TEXT]], [[None], ["x"]])
        cur = await aconn.execute("SELECT v FROM t")
        assert cur.description is not None
        assert cur.description[0][1] == TEXT

    async def test_empty_result_uses_unknown_sentinel(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.query_reply = (["v"], [], [], [])
        cur = await aconn.execute("SELECT v FROM t WHERE 0")
        assert cur.description is not None
        assert cur.description[0][1] == UNKNOWN
        assert cur.rowcount == 0
        assert await cur.fetchone() is None

    async def test_datetime_cells_are_decoded(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.query_reply = (
            ["when", "epoch"],
            [ISO, UNIX],
            [[ISO, UNIX]],
            [["2024-01-02 03:04:05", 86400]],
        )
        cur = await aconn.execute("SELECT when, epoch FROM t")
        assert await cur.fetchone() == (
            datetime.datetime(2024, 1, 2, 3, 4, 5),
            datetime.datetime(1970, 1, 2, tzinfo=datetime.UTC),
        )

    async def test_pragma_read_reports_no_rowcount(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.query_reply = (["foreign_keys"], [INT], [[INT]], [[1]])
        cur = await aconn.execute("PRAGMA foreign_keys")
        assert cur.rowcount == -1
        assert await cur.fetchone() == (1,)

    async def test_pragma_write_has_no_result_set(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        cur = await aconn.execute("PRAGMA foreign_keys = OFF")
        assert cur.description is None
        assert await cur.fetchall() == []


class TestExecResults:
    async def test_insert_sets_lastrowid_and_rowcount(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.exec_reply = (7, 1)
        cur = await aconn.execute("INSERT INTO t VALUES (?)", (1,))
        assert (cur.lastrowid, cur.rowcount, cur.description) == (7, 1, None)

    async def test_negative_rowid_arrives_unsigned_on_the_wire(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.exec_reply = (2**64 - 5, 1)
        cur = await aconn.execute("INSERT INTO t VALUES (1)")
        assert cur.lastrowid == -5

    async def test_lastrowid_sticks_through_other_statements(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        cur = aconn.cursor()
        stub.exec_reply = (3, 1)
        await cur.execute("INSERT INTO t VALUES (1)")
        stub.exec_reply = (0, 4)
        await cur.execute("UPDATE t SET a = 1")
        assert (cur.lastrowid, cur.rowcount) == (3, 4)
        await cur.execute("CREATE TABLE u (x)")
        assert (cur.lastrowid, cur.rowcount) == (3, -1)

    async def test_parameters_are_adapted(self, aconn: AsyncConnection, stub: StubClient) -> None:
        await aconn.execute("INSERT INTO t VALUES (?, ?)", (datetime.date(2024, 1, 2), True))
        assert stub.statements[-1][1] == ["2024-01-02", True]


class TestErrors:
    async def test_constraint_violation(self, aconn: AsyncConnection, stub: StubClient) -> None:
        stub.errors.append(server_error(SQLITE_CONSTRAINT, "UNIQUE failed"))
        with pytest.raises(IntegrityError, match="UNIQUE failed") as info:
            await aconn.execute("INSERT INTO t VALUES (1)")
        assert info.value.code == SQLITE_CONSTRAINT
        assert not aconn.closed

    async def test_lost_wire_session_closes_the_connection(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.disconnect_on_error = True
        stub.errors.append(client_exc.DqliteConnectionError("peer went away"))
        with pytest.raises(OperationalError, match="peer went away"):
            await aconn.execute("SELECT 1")
        assert aconn.closed and aconn.invalidated
        with pytest.raises(InterfaceError, match="invalidated"):
            await aconn.execute("SELECT 1")
        with pytest.raises(InterfaceError, match="invalidated"):
            aconn.cursor()

    async def test_busy_is_retried_until_it_succeeds(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.errors.extend([server_error(SQLITE_BUSY, "busy"), server_error(SQLITE_BUSY, "busy")])
        await aconn.execute("INSERT INTO t VALUES (1)")
        assert len(stub.statements) == 3

    async def test_busy_budget_zero_does_not_retry(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        aconn.busy_timeout = 0
        stub.errors.append(server_error(SQLITE_BUSY, "busy"))
        with pytest.raises(OperationalError, match="busy"):
            await aconn.execute("INSERT INTO t VALUES (1)")
        assert len(stub.statements) == 1

    async def test_busy_budget_exhausted_reraises(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.errors.extend(server_error(SQLITE_BUSY, "busy") for _ in range(20))
        with pytest.raises(OperationalError, match="busy"):
            await aconn.execute("INSERT INTO t VALUES (1)")
        assert 2 <= len(stub.statements) < 20


class TestTransactions:
    async def test_commit_without_transaction_sends_nothing(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        await aconn.commit()
        await aconn.rollback()
        assert stub.sql == []

    async def test_commit_sends_commit(self, aconn: AsyncConnection, stub: StubClient) -> None:
        await aconn.execute("BEGIN")
        assert aconn.in_transaction
        await aconn.commit()
        assert stub.sql == ["BEGIN IMMEDIATE", "COMMIT"]
        assert not aconn.in_transaction

    async def test_no_transaction_reply_is_swallowed(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        await aconn.execute("BEGIN")
        stub.errors.append(server_error(SQLITE_ERROR, "cannot commit - no transaction is active"))
        await aconn.commit()

    async def test_other_commit_errors_propagate(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        await aconn.execute("BEGIN")
        stub.errors.append(server_error(SQLITE_CONSTRAINT, "FOREIGN KEY constraint failed"))
        with pytest.raises(IntegrityError):
            await aconn.commit()

    async def test_leadership_lost_during_commit_is_ambiguous(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        await aconn.execute("BEGIN")
        stub.errors.append(server_error(LEADERSHIP_LOST, "leadership lost"))
        with pytest.raises(AmbiguousCommitError) as info:
            await aconn.commit()
        assert info.value.code == LEADERSHIP_LOST

    async def test_leadership_lost_on_explicit_commit_statement_is_ambiguous(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        await aconn.execute("BEGIN")
        stub.errors.append(server_error(LEADERSHIP_LOST, "leadership lost"))
        with pytest.raises(AmbiguousCommitError):
            await aconn.execute("COMMIT")

    async def test_not_leader_during_commit_is_a_plain_failure(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        await aconn.execute("BEGIN")
        stub.errors.append(server_error(NOT_LEADER, "not leader"))
        with pytest.raises(OperationalError) as info:
            await aconn.commit()
        assert type(info.value) is OperationalError

    async def test_leadership_lost_on_insert_is_not_ambiguous(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.errors.append(server_error(LEADERSHIP_LOST, "leadership lost"))
        with pytest.raises(OperationalError) as info:
            await aconn.execute("INSERT INTO t VALUES (1)")
        assert type(info.value) is OperationalError

    async def test_transaction_block_commits(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        async with aconn.transaction():
            await aconn.execute("INSERT INTO t VALUES (1)")
        assert stub.sql == ["BEGIN IMMEDIATE", "INSERT INTO t VALUES (1)", "COMMIT"]

    async def test_transaction_block_rolls_back(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        with pytest.raises(RuntimeError, match="boom"):
            async with aconn.transaction():
                raise RuntimeError("boom")
        assert stub.sql == ["BEGIN IMMEDIATE", "ROLLBACK"]

    async def test_stray_commit_inside_transaction_block(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        async with aconn.transaction():
            with pytest.raises(InterfaceError, match="transaction\\(\\)"):
                await aconn.commit()
            with pytest.raises(InterfaceError, match="Nested"):
                async with aconn.transaction():
                    pass

    async def test_context_manager_commits_and_rolls_back(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        async with aconn:
            await aconn.execute("BEGIN")
        assert stub.sql[-1] == "COMMIT"
        with pytest.raises(ValueError):
            async with aconn:
                await aconn.execute("BEGIN")
                raise ValueError
        assert stub.sql[-1] == "ROLLBACK"
        assert not aconn.closed


class TestSessionMode:
    @pytest.mark.parametrize(
        ("mode", "begin"),
        [("immediate", "BEGIN IMMEDIATE"), ("exclusive", "BEGIN EXCLUSIVE"), ("deferred", "BEGIN")],
    )
    async def test_bare_begin_follows_mode(self, stub: StubClient, mode: str, begin: str) -> None:
        aconn = AsyncConnection("localhost:19001", session_mode=mode)
        aconn._client = stub  # type: ignore[assignment]
        await aconn.execute("BEGIN")
        assert stub.sql == [begin]

    async def test_switching_to_read_only_emits_pragma(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        await aconn.set_session_mode("read_only")
        await aconn.set_session_mode("deferred")
        await aconn.set_session_mode("immediate")
        assert stub.sql == ["PRAGMA query_only = 1", "PRAGMA query_only = 0"]
        assert aconn.session_mode == "immediate" and aconn.default_session_mode == "immediate"

    async def test_cannot_switch_inside_transaction(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        await aconn.execute("BEGIN")
        with pytest.raises(InterfaceError, match="transaction"):
            await aconn.set_session_mode("read_only")

    async def test_invalid_mode(self, aconn: AsyncConnection) -> None:
        with pytest.raises(ProgrammingError, match="Invalid session_mode"):
            await aconn.set_session_mode("turbo")


class TestBusyTimeoutPragma:
    async def test_intercepted_without_wire_round_trip(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        cur = await aconn.execute("PRAGMA busy_timeout = 250")
        assert await cur.fetchone() == (250,)
        assert aconn.busy_timeout == 0.25
        cur = await aconn.execute("PRAGMA busy_timeout")
        assert await cur.fetchone() == (250,)
        assert stub.sql == []


class TestFetching:
    async def test_fetch_semantics(self, aconn: AsyncConnection, stub: StubClient) -> None:
        stub.query_reply = (["n"], [INT], [[INT]] * 5, [[i] for i in range(5)])
        cur = await aconn.execute("SELECT n FROM t")
        assert cur.rownumber == 0
        assert await cur.fetchmany(0) == []
        assert await cur.fetchmany(2) == [(0,), (1,)]
        cur.arraysize = 2
        assert await cur.fetchmany() == [(2,), (3,)]
        assert [r async for r in cur] == [(4,)]
        assert await cur.fetchone() is None
        assert cur.rownumber == 5

    async def test_fetchmany_rejects_bad_sizes(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.query_reply = (["n"], [INT], [[INT]], [[1]])
        cur = await aconn.execute("SELECT n FROM t")
        for bad in (-1, True, "2"):
            with pytest.raises(ProgrammingError):
                await cur.fetchmany(bad)  # type: ignore[arg-type]

    async def test_fetch_after_dml_returns_nothing(self, aconn: AsyncConnection) -> None:
        cur = await aconn.execute("DELETE FROM t")
        assert await cur.fetchone() is None
        assert await cur.fetchmany() == []
        assert await cur.fetchall() == []

    async def test_row_factory_receives_cursor(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.query_reply = (["id", "name"], [INT, TEXT], [[INT, TEXT]], [[1, "a"]])
        aconn.row_factory = Row
        cur = await aconn.execute("SELECT id, name FROM t")
        row = await cur.fetchone()
        assert isinstance(row, Row) and row["name"] == "a" and row[0] == 1

    async def test_row_factory_type_error_becomes_data_error(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.query_reply = (["n"], [INT], [[INT]], [[1]])
        cur = await aconn.execute("SELECT n FROM t")
        cur.row_factory = lambda: None  # wrong arity
        with pytest.raises(DataError, match="row_factory"):
            await cur.fetchone()

    async def test_closed_cursor_keeps_counters_but_cannot_fetch(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.exec_reply = (9, 1)
        cur = await aconn.execute("INSERT INTO t VALUES (1)")
        cur.close()
        cur.close()
        assert (cur.lastrowid, cur.rowcount, cur.description) == (9, 1, None)
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cur.fetchone()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cur.execute("SELECT 1")
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cur.scroll(1)


class TestExecutemany:
    async def test_rowcount_accumulates(self, aconn: AsyncConnection, stub: StubClient) -> None:
        stub.exec_reply = (5, 1)
        cur = await aconn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
        assert cur.rowcount == 3
        assert cur.completed_iterations == 3
        assert cur.lastrowid is None
        assert [p for _, p in stub.statements] == [[1], [2], [3]]

    async def test_returning_rows_accumulate(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.query_reply = (["id"], [INT], [[INT]], [[1]])
        cur = await aconn.executemany("INSERT INTO t VALUES (?) RETURNING id", [(1,), (2,)])
        assert await cur.fetchall() == [(1,), (1,)]
        assert cur.rowcount == 2

    async def test_empty_sequence(self, aconn: AsyncConnection, stub: StubClient) -> None:
        cur = await aconn.executemany("INSERT INTO t VALUES (?)", [])
        assert cur.rowcount == 0 and stub.sql == []

    async def test_failure_mid_batch_resets_state(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        stub.exec_reply = (42, 1)
        cur = await aconn.execute("INSERT INTO t VALUES (0)")
        stub.errors = [server_error(SQLITE_CONSTRAINT, "dup")]
        with pytest.raises(IntegrityError):
            await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
        assert cur.rowcount == -1
        assert cur.lastrowid == 42
        assert await cur.fetchall() == []

    @pytest.mark.parametrize(
        ("sql", "seq", "match"),
        [
            ("SELECT ?", [(1,)], "only accepts"),
            ("PRAGMA foreign_keys = ?", [(1,)], "only accepts"),
            ("BEGIN", [()], "does not accept BEGIN"),
            ("INSERT INTO t VALUES (?)", None, "not None"),
            ("INSERT INTO t VALUES (?)", {"a": 1}, "parameter sets"),
            ("INSERT INTO t VALUES (?)", "ab", "parameter sets"),
            ("INSERT INTO t VALUES (?, ?)", [(1,)], "Incorrect number of bindings"),
        ],
    )
    async def test_rejections_happen_before_the_wire(
        self, aconn: AsyncConnection, stub: StubClient, sql: str, seq: Any, match: str
    ) -> None:
        with pytest.raises(ProgrammingError, match=match):
            await aconn.executemany(sql, seq)
        assert stub.sql == []


class TestConnectionState:
    async def test_closed_connection_rejects_everything(
        self, aconn: AsyncConnection, stub: StubClient
    ) -> None:
        cur = aconn.cursor()
        await aconn.close()
        await aconn.close()
        assert aconn.closed and not aconn.invalidated and not stub.is_connected
        assert cur.closed
        with pytest.raises(InterfaceError, match="closed"):
            aconn.cursor()
        with pytest.raises(InterfaceError, match="closed"):
            await aconn.commit()

    async def test_fork_is_detected(
        self, aconn: AsyncConnection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(os, "getpid", lambda: 4_000_000)
        with pytest.raises(InterfaceError, match="used after fork"):
            await aconn.execute("SELECT 1")

    async def test_autocommit_and_isolation_level(self, aconn: AsyncConnection) -> None:
        assert aconn.autocommit is True
        aconn.autocommit = -1
        assert aconn.autocommit == -1
        with pytest.raises(NotSupportedError):
            aconn.autocommit = False
        aconn.isolation_level = "deferred"
        assert aconn.isolation_level == "DEFERRED"
        with pytest.raises(ProgrammingError):
            aconn.isolation_level = "SERIALIZABLE"
        with pytest.raises(NotSupportedError):
            aconn.text_factory = bytes


class TestSyncSurface:
    def test_query_and_fetch(self, conn: dqlitedbapi.Connection, stub: StubClient) -> None:
        stub.query_reply = (["n"], [INT], [[INT]] * 3, [[1], [2], [3]])
        cur = conn.execute("SELECT n FROM t")
        assert cur.fetchone() == (1,)
        assert list(cur) == [(2,), (3,)]
        assert cur.rowcount == 3

    def test_commit_rollback_and_ambiguous_commit(
        self, conn: dqlitedbapi.Connection, stub: StubClient
    ) -> None:
        conn.execute("BEGIN")
        assert conn.in_transaction
        conn.rollback()
        assert stub.sql == ["BEGIN IMMEDIATE", "ROLLBACK"]
        conn.execute("BEGIN")
        stub.errors.append(server_error(LEADERSHIP_LOST, "leadership lost"))
        with pytest.raises(AmbiguousCommitError):
            conn.commit()

    def test_transaction_block(self, conn: dqlitedbapi.Connection, stub: StubClient) -> None:
        with conn.transaction():
            conn.execute("INSERT INTO t VALUES (1)")
        with pytest.raises(KeyError), conn.transaction():
            raise KeyError("x")
        assert stub.sql == [
            "BEGIN IMMEDIATE",
            "INSERT INTO t VALUES (1)",
            "COMMIT",
            "BEGIN IMMEDIATE",
            "ROLLBACK",
        ]

    def test_executemany_and_row_factory(
        self, conn: dqlitedbapi.Connection, stub: StubClient
    ) -> None:
        stub.exec_reply = (1, 1)
        assert conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)]).rowcount == 2
        stub.query_reply = (["id"], [INT], [[INT]], [[1]])
        conn.row_factory = Row
        row = conn.execute("SELECT id FROM t").fetchone()
        assert isinstance(row, Row) and row["id"] == 1

    def test_lost_session_closes_connection(
        self, conn: dqlitedbapi.Connection, stub: StubClient
    ) -> None:
        stub.disconnect_on_error = True
        stub.errors.append(client_exc.DqliteConnectionError("gone"))
        with pytest.raises(OperationalError):
            conn.execute("SELECT 1")
        assert conn.closed and conn.invalidated
        with pytest.raises(InterfaceError):
            conn.cursor()

    def test_unused_connection_context_manager_is_silent(self) -> None:
        with dqlitedbapi.connect("127.0.0.1:1") as unused:
            pass
        assert not unused.closed
        unused.close()
