"""Async cursor implementation for dqlite."""

from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

from dqlitedbapi.cursor import (
    _call_client,
    _convert_params,
    _convert_row,
    _ExecuteManyAccumulator,
    _is_dml_with_returning,
    _is_row_returning,
)
from dqlitedbapi.exceptions import InterfaceError, NotSupportedError, ProgrammingError
from dqlitedbapi.types import _Description

if TYPE_CHECKING:
    from dqlitedbapi.aio.connection import AsyncConnection


__all__ = ["AsyncCursor"]


class AsyncCursor:
    """Async database cursor."""

    # Mirrors ``Cursor.__slots__`` in the sync tree: stable attribute
    # set, allocated one per ``AsyncConnection.cursor()`` call.
    __slots__ = (
        "_arraysize",
        "_closed",
        "_connection",
        "_description",
        "_lastrowid",
        "_row_index",
        "_rowcount",
        "_rows",
        "messages",
    )

    def __init__(self, connection: "AsyncConnection") -> None:
        self._connection = connection
        self._description: _Description = None
        self._rowcount = -1
        self._arraysize = 1
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self._closed = False
        self._lastrowid: int | None = None
        # PEP 249 optional extension; see Cursor.messages.
        self.messages: list[tuple[type[Exception], Exception | str]] = []

    @property
    def connection(self) -> "AsyncConnection":
        """The AsyncConnection this cursor was created from.

        PEP 249 optional extension. Read-only.
        """
        return self._connection

    @property
    def description(self) -> _Description:
        """Column descriptions for the last query.

        Returns a tuple of 7-tuples:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)

        ``type_code`` is the wire-level ``ValueType`` integer from the first
        result frame (e.g. 10 for ISO8601, 9 for UNIXTIME). The other fields
        are None — dqlite doesn't expose them.

        Returns the same tuple object on each access (matching stdlib
        ``sqlite3.Cursor.description``). A tuple is structurally
        immutable so no defensive copy is needed to keep the cursor's
        internal state safe from caller mutation.
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """Number of rows affected by the last execute.

        Returns -1 if not applicable or unknown.
        """
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        """ROWID of this cursor's most-recent successful INSERT.

        Returns ``None`` before the first INSERT runs on this cursor
        and after ``close()`` scrubs the cursor's state.

        Unlike ``sqlite3.Cursor.lastrowid``, the value is scoped to the
        cursor, not the underlying AsyncConnection: a sibling cursor on
        the same connection will not observe this cursor's last INSERT.
        The scrub on ``close()`` is consistent with that scope — ROLLBACK
        / UPDATE / DELETE / DDL do NOT clear it (mirroring stdlib), but
        closing the cursor does.

        **Not updated for ``INSERT ... RETURNING``** (or any row-returning
        statement). dqlite's wire protocol does not return
        ``last_insert_id`` on row-returning responses, so the
        row-returning execute path cannot surface the rowid. Read the
        id from the returned row instead. This is a known divergence
        from ``sqlite3.Cursor.lastrowid`` which updates after
        ``INSERT ... RETURNING``.
        """
        return self._lastrowid

    @property
    def rownumber(self) -> int | None:
        """0-based index of the next row in the current result set.

        PEP 249 optional extension: returns ``None`` if no result set is
        active (no query executed, or last statement was DML without
        RETURNING); otherwise returns the index of the row that the next
        ``fetchone()`` would produce.
        """
        if self._description is None:
            return None
        return self._row_index

    @property
    def arraysize(self) -> int:
        """Number of rows to fetch at a time with fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        # Reject bools explicitly even though ``bool`` is an ``int``
        # subclass: ``arraysize = True`` silently coercing to 1 is a
        # caller-bug trap, not a useful affordance.
        if not isinstance(value, int) or isinstance(value, bool):
            raise ProgrammingError(f"arraysize must be a positive int, got {type(value).__name__}")
        if value < 1:
            raise ProgrammingError(f"arraysize must be >= 1, got {value}")
        self._arraysize = value

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError("Cursor is closed")

    def _reset_execute_state(self) -> None:
        """Clear per-execute state to the "no result set" baseline.

        Mirrors the sync ``Cursor._reset_execute_state`` — see that
        docstring. These are synchronous attribute writes on one
        cursor instance; they deliberately happen OUTSIDE ``op_lock``
        because the lock exists to serialise access to the underlying
        wire connection, not to the cursor's in-memory fields.
        ``_lastrowid`` is cursor-scoped but survives across execute —
        only ``close()`` scrubs it (see the ``lastrowid`` property).
        """
        self._description = None
        self._rows = []
        self._row_index = 0
        self._rowcount = -1

    async def execute(
        self, operation: str, parameters: Sequence[Any] | None = None
    ) -> "AsyncCursor":
        """Execute a database operation (query or command).

        Returns ``self`` so callers can chain ``.fetchall()`` etc.
        """
        # PEP 249 §6.1.2: ``messages`` is cleared by every standard
        # cursor method before the call runs.
        del self.messages[:]
        # Fast-path guard outside the lock so we fail quickly on an
        # already-closed cursor without taking the lock.
        self._check_closed()
        # Clear state after the closed guard and before taking the
        # lock: matches stdlib sqlite3 semantics so a mid-execute
        # failure (including CancelledError) leaves the cursor in the
        # "no result set" baseline rather than reporting the prior
        # query's description.
        self._reset_execute_state()

        is_query = _is_row_returning(operation)
        params = _convert_params(parameters)
        _, op_lock = self._connection._ensure_locks()
        async with op_lock:
            # Re-check after acquiring the lock so that a concurrent
            # ``cursor.close()`` / ``connection.close()`` that reaches the
            # closed flag first wins deterministically. Without the
            # re-check, a cursor closed between the fast-path guard and
            # the lock acquisition reports the race as
            # "connection has been invalidated" or "protocol is None"
            # rather than the sharper "Cursor is closed" / "Connection
            # is closed" that the caller expects.
            self._check_closed()
            conn = await self._connection._ensure_connection()
            # ``_ensure_connection`` awaits, so close() can still race
            # against this window. Re-check once more before touching
            # the wire.
            self._check_closed()
            if is_query:
                columns, column_types, row_types, rows = await _call_client(
                    conn.query_raw_typed(operation, params)
                )
                if not columns:
                    # PRAGMA write-form dispatches through the row-
                    # returning branch but produces no columns; match
                    # stdlib sqlite3's ``description is None`` contract
                    # for non-result statements. See the sync
                    # ``_execute_async`` companion for rationale.
                    self._description = None
                else:
                    self._description = tuple(
                        (
                            name,
                            column_types[i] if i < len(column_types) else None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                        for i, name in enumerate(columns)
                    )
                # Per-row dispatch; see the sync ``_execute_async``
                # companion for the rationale.
                self._rows = [
                    _convert_row(row, row_types[i] if i < len(row_types) else column_types)
                    for i, row in enumerate(rows)
                ]
                self._row_index = 0
                self._rowcount = len(rows)
            else:
                last_id, affected = await _call_client(conn.execute(operation, params))
                self._lastrowid = last_id
                self._rowcount = affected
                self._description = None
                self._rows = []
                # Parity with the SELECT branch and with executemany:
                # every execute must leave the cursor at row 0 of its
                # (possibly empty) result set so a subsequent SELECT
                # iterator starts from a clean state.
                self._row_index = 0

        return self

    async def executemany(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]]
    ) -> "AsyncCursor":
        """Execute a database operation multiple times.

        An empty ``seq_of_parameters`` must not leave stale SELECT
        state around: reset description / rows so callers can't
        confuse an empty executemany with a preceding SELECT.

        For statements with a RETURNING clause, rows produced by each
        iteration are accumulated into ``_rows`` so a subsequent
        ``fetchall`` yields every returned row across parameter sets.

        Pure queries (SELECT / VALUES / PRAGMA) are rejected before the
        loop runs — stdlib ``sqlite3.Cursor.executemany`` does the same.
        INSERT / UPDATE / DELETE / REPLACE (with or without RETURNING)
        remain admitted.
        """
        del self.messages[:]
        self._check_closed()
        if _is_row_returning(operation) and not _is_dml_with_returning(operation):
            raise ProgrammingError(
                "executemany() can only execute DML statements; "
                "use execute() for SELECT / VALUES / PRAGMA / EXPLAIN / WITH."
            )

        # Single source of truth for per-execute reset; see
        # ``_reset_execute_state``. Also zeroes ``_rowcount`` to -1 so
        # an empty ``seq_of_parameters`` ends with the same
        # ``rowcount`` shape as empty ``execute``.
        self._reset_execute_state()
        acc = _ExecuteManyAccumulator(max_rows=self._connection._max_total_rows)
        for params in seq_of_parameters:
            # Re-check before each iteration so a concurrent close()
            # landing between iterations surfaces as "Cursor is
            # closed" rather than being observed only on the next
            # iteration's nested execute entry (or not at all for a
            # single-iteration remainder).
            self._check_closed()
            await self.execute(operation, params)
            self._check_closed()
            acc.push(self)
        # Final guard before apply; pairs with the ``_closed`` check
        # inside ``_ExecuteManyAccumulator.apply``.
        self._check_closed()
        acc.apply(self)
        return self

    def _check_result_set(self) -> None:
        if self._description is None:
            raise ProgrammingError("no results to fetch; execute a query first")

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set.

        Returns ``None`` when no more rows are available.
        """
        del self.messages[:]
        self._check_closed()
        self._check_result_set()
        # PEP 249 §6.1.1 — Connection.messages is cleared by the
        # cursor fetch methods. Defensive against test mocks.
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]

        if self._row_index >= len(self._rows):
            return None

        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to ``size`` next rows of a query result.

        Returns an empty list when no more rows are available. ``size``
        defaults to ``self.arraysize``.
        """
        del self.messages[:]
        self._check_closed()
        self._check_result_set()
        # PEP 249 §6.1.1 — Connection.messages is cleared by the
        # cursor fetch methods. Defensive against test mocks.
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]

        if size is None:
            size = self._arraysize
        if size < 0:
            # See sync Cursor.fetchmany: silently returning [] on a
            # negative size hides caller bugs.
            raise ProgrammingError(f"fetchmany size must be non-negative, got {size}")

        result: list[tuple[Any, ...]] = []
        for _ in range(size):
            row = await self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result.

        Returns an empty list when the cursor has no more rows.
        """
        del self.messages[:]
        self._check_closed()
        self._check_result_set()
        # PEP 249 §6.1.1 — Connection.messages is cleared by the
        # cursor fetch methods. Defensive against test mocks.
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]

        result = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return result

    async def close(self) -> None:
        """Close the cursor.

        Idempotent: a second call is a no-op.
        """
        if self._closed:
            return
        self._closed = True
        self._rows = []
        self._description = None
        # Scrub the remaining state fields so every post-close reader
        # sees a consistent "no operation performed" surface. Symmetric
        # with ``Cursor.close()``.
        self._rowcount = -1
        self._lastrowid = None

    def setinputsizes(self, sizes: Sequence[int | None]) -> None:
        """Set input sizes (no-op for dqlite).

        PEP 249 §6.1.1 names ``setinputsizes`` among the methods that
        clear the ``messages`` list; we do so even though the method
        itself does no work.
        """
        # PEP 249 §6.1.2 — closed-cursor operations raise.
        self._check_closed()
        del self.messages[:]
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Set output size (no-op for dqlite). See ``setinputsizes``."""
        # PEP 249 §6.1.2 — closed-cursor operations raise.
        self._check_closed()
        del self.messages[:]
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]

    def callproc(
        self, procname: str, parameters: Sequence[Any] | None = None
    ) -> Sequence[Any] | None:
        """PEP 249 optional extension — not supported.

        Sync despite the cursor being async: the method raises
        unconditionally, so wrapping it in a coroutine has no value and
        would diverge from the sync siblings (``nextset`` / ``scroll``)
        and from the SQLAlchemy adapter (``sqlalchemy-dqlite``), which
        both expose these as plain methods.
        """
        # PEP 249 §6.1.2 — closed-cursor ops raise.
        self._check_closed()
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> bool | None:
        """PEP 249 optional extension — not supported."""
        # PEP 249 §6.1.2 — closed-cursor ops raise.
        self._check_closed()
        # PEP 249 §6.1.1 names ``nextset`` among the cursor methods
        # that clear ``Connection.messages``; clear before raising so
        # the contract holds even on the not-supported path.
        del self.messages[:]
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative") -> None:
        """PEP 249 optional extension — not supported."""
        # PEP 249 §6.1.2 — closed-cursor ops raise.
        self._check_closed()
        raise NotSupportedError("dqlite cursors are not scrollable")

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<AsyncCursor rowcount={self._rowcount} {state}>"

    def __aiter__(self) -> "AsyncCursor":
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
