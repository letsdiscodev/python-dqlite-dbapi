"""Async cursor implementation for dqlite."""

import contextlib
import weakref
from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, NoReturn, Self

from dqlitedbapi.cursor import (
    _EXECUTEMANY_REJECT_VERBS,
    _call_client,
    _classify_caller_sql,
    _convert_params,
    _convert_row,
    _ExecuteManyAccumulator,
    _is_dml_with_returning,
    _is_insert_or_replace,
    _is_row_returning,
    _strip_leading_comments,
    _to_signed_int64,
)
from dqlitedbapi.exceptions import (
    DataError,
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
)
from dqlitedbapi.types import _Description

if TYPE_CHECKING:
    from dqlitedbapi.aio.connection import AsyncConnection


__all__ = ["AsyncCursor"]


class AsyncCursor:
    """Async database cursor."""

    # Mirrors ``Cursor.__slots__`` in the sync tree: stable attribute
    # set, allocated one per ``AsyncConnection.cursor()`` call.
    # ``__weakref__`` lets ``AsyncConnection._cursors`` (a WeakSet)
    # hold a reference for the close-cascade.
    __slots__ = (
        "__weakref__",
        "_arraysize",
        "_closed",
        "_connection",
        "_description",
        "_lastrowid",
        "_row_factory",
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
        # Inherit parent connection's default row_factory (stdlib
        # parity). Class-name check restricts inheritance to real
        # AsyncConnection instances — MagicMock-typed test fakes
        # would otherwise silently wrap every row.
        self._row_factory: Any = (
            getattr(connection, "_row_factory", None)
            if type(connection).__name__ == "AsyncConnection"
            else None
        )
        # PEP 249 optional extension; see Cursor.messages.
        self.messages: list[tuple[type[Exception], Exception | str]] = []

    @property
    def connection(self) -> "AsyncConnection":
        """The AsyncConnection this cursor was created from.

        PEP 249 optional extension. Read-only.

        After ``close()``, ``self._connection`` is swapped for a
        ``weakref.proxy``. Once the AsyncConnection is itself GC'd,
        attribute access on the proxy raises ``ReferenceError`` —
        outside the PEP 249 ``Error`` hierarchy. Catch and re-raise
        as ``InterfaceError`` so cross-driver code wrapping cursor
        introspection in ``except dbapi.Error:`` continues to match.
        """
        try:
            _ = self._connection.address
        except ReferenceError as e:
            raise InterfaceError(
                "Cursor's parent AsyncConnection has been garbage-collected"
            ) from e
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

        Cursor-scoped, matching stdlib ``sqlite3.Cursor.lastrowid``: a
        sibling cursor on the same AsyncConnection does NOT observe
        this cursor's last INSERT (each cursor stores its own snapshot
        captured at INSERT time from the underlying connection's
        ``sqlite3_last_insert_rowid``). ROLLBACK / UPDATE / DELETE /
        DDL do NOT clear it (mirroring stdlib), but ``close()``
        scrubs it as part of the closed-cursor "no operation
        performed" surface contract.

        **Not updated for ``INSERT ... RETURNING``** (or any row-returning
        statement). dqlite's wire protocol does not return
        ``last_insert_id`` on row-returning responses, so the
        row-returning execute path cannot surface the rowid. Read the
        id from the returned row instead. This IS a divergence from
        stdlib ``sqlite3.Cursor.lastrowid``, which updates after
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
        # PEP 249 §6.1.2: any state-mutating method on a closed cursor
        # must raise an ``Error`` subclass. Apply the closed-state
        # guard FIRST so a bool/int validation error doesn't shadow
        # the closed-cursor error. Mirrors the sync sibling at
        # ``cursor.py:920-935``.
        self._check_closed()
        # Reject bools explicitly even though ``bool`` is an ``int``
        # subclass: ``arraysize = True`` silently coercing to 1 is a
        # caller-bug trap, not a useful affordance.
        if not isinstance(value, int) or isinstance(value, bool):
            raise ProgrammingError(f"arraysize must be a positive int, got {type(value).__name__}")
        if value < 1:
            raise ProgrammingError(f"arraysize must be >= 1, got {value}")
        self._arraysize = value

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` has been called.

        Peer-driver parity (psycopg, asyncpg). PEP 249 does not
        require it; the underlying flag is already maintained.
        """
        return self._closed

    @property
    def row_factory(self) -> Any:
        """stdlib ``sqlite3.Cursor.row_factory`` parity hook. See
        sync sibling ``Cursor.row_factory`` for full docs."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        self._check_closed()
        if value is not None and not callable(value):
            raise ProgrammingError(
                f"row_factory must be callable or None, got {type(value).__name__}"
            )
        self._row_factory = value

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError(f"Cursor is closed (id={id(self)})")

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

    async def _execute_unlocked(
        self, operation: str, parameters: Sequence[Any] | None = None
    ) -> None:
        """Body of a single ``execute`` call — caller already holds ``op_lock``.

        Factored out so ``executemany`` can hold the lock once across
        every iteration rather than dropping and re-taking it per
        parameter set (the per-iteration drop used to let a concurrent
        task on the same connection slip arbitrary statements between
        iterations — including COMMIT / ROLLBACK / DDL). Caller is
        responsible for:

        - clearing ``messages``,
        - holding ``op_lock``,
        - pre- and post-check ``_check_closed()``,
        - resetting execute state when this is the first iteration.
        """
        is_query = _is_row_returning(operation)
        params = _convert_params(parameters)
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
                # stdlib sqlite3's ``description = None`` /
                # ``rowcount = -1`` contract for non-result statements.
                # See the sync ``_execute_async`` companion for
                # rationale.
                self._description = None
                self._rows = []
                self._row_index = 0
                self._rowcount = -1
                return
            else:
                # PEP 249 §6.1.2 ``type_code`` must compare equal to a
                # Type Object. See the sync ``_execute_async`` for
                # the full rationale. Empty result set → column_types
                # is legitimately empty and the wire does not carry
                # declared column affinity separately from the
                # per-row type tags, so the type information is
                # unrecoverable. We emit ``None`` as a documented
                # deviation; any synthesised value would mislead in a
                # different direction. Callers that need column-type
                # introspection on empty result sets should issue
                # ``PRAGMA table_info(...)`` separately. Non-empty
                # but short → ``DataError`` so the anomaly surfaces
                # loudly.
                if len(column_types) == 0 and len(rows) == 0:
                    type_codes: list[Any] = [None] * len(columns)
                elif len(column_types) != len(columns):
                    raise DataError(
                        f"Wire response has {len(columns)} columns but "
                        f"{len(column_types)} type codes"
                    )
                else:
                    # Map ValueType.NULL → None to satisfy PEP 249
                    # §6.1.2 ("type_code must compare equal to one of
                    # Type Objects"). See sync sibling rationale.
                    from dqlitewire.constants import ValueType as _VT

                    type_codes = [None if c == _VT.NULL else c for c in column_types]
                self._description = tuple(
                    (name, type_codes[i], None, None, None, None, None)
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
            # stdlib-parity: lastrowid only updates on INSERT / REPLACE.
            # See ``_is_insert_or_replace`` in the sync cursor for
            # rationale — sync and async share the same contract.
            if _is_insert_or_replace(operation):
                self._lastrowid = _to_signed_int64(last_id)
            self._rowcount = _to_signed_int64(affected)
            self._description = None
            self._rows = []
            # Parity with the SELECT branch and with executemany:
            # every execute must leave the cursor at row 0 of its
            # (possibly empty) result set so a subsequent SELECT
            # iterator starts from a clean state.
            self._row_index = 0

    async def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> Self:
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

        # Pre-flight classification of caller-supplied SQL — empty /
        # multi-statement / wrong ``?``-count. Mirrors the sync sibling
        # at cursor.py. See ``_classify_caller_sql`` docstring.
        _classify_caller_sql(operation, parameters)

        _, op_lock = self._connection._ensure_locks()
        async with op_lock:
            # PEP 249 §6.1.1 — clear messages under the lock so the
            # contract "messages cleared by every method call" is
            # atomic with the operation. Clearing only pre-lock leaves
            # a window where a sibling task could append between this
            # clear and the EXEC. Mirrors the commit/rollback
            # discipline.
            del self.messages[:]
            # Re-check after acquiring the lock so that a concurrent
            # ``cursor.close()`` / ``connection.close()`` that reaches the
            # closed flag first wins deterministically. Without the
            # re-check, a cursor closed between the fast-path guard and
            # the lock acquisition reports the race as
            # "connection has been invalidated" or "protocol is None"
            # rather than the sharper "Cursor is closed" / "Connection
            # is closed" that the caller expects.
            self._check_closed()
            await self._execute_unlocked(operation, parameters)

        return self

    async def executemany(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]], /
    ) -> Self:
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

        Cancellation atomicity: this driver runs in autocommit-by-default
        mode. Without a surrounding ``BEGIN`` ... ``COMMIT`` (or a
        client-layer ``transaction()`` ctxmgr / SA-engine
        transaction), each iteration commits server-side independently.
        If the surrounding task is cancelled mid-batch (``asyncio.timeout``,
        ``asyncio.shield`` expiry, etc.), the iterations that already
        completed remain persisted; partial-batch persistence is the
        consequence of running outside a transaction. To make the
        batch atomic, wrap the call in an explicit ``BEGIN`` /
        ``COMMIT``. See the ``Connection`` class docstring for the
        autocommit-by-default rationale.
        """
        del self.messages[:]
        self._check_closed()
        # Reject transaction-control verbs and pure queries up front
        # (mirror of the sync sibling).
        # See sync sibling for the leading ``;``-stripping loop and the
        # trailing ``rstrip(";")`` rationale.
        # Loop comment-strip + ;-strip together so a leading ``;``
        # followed by a comment does not bypass the reject-list. See
        # the sync sibling for full rationale.
        head_normalised = operation
        while True:
            stripped = _strip_leading_comments(head_normalised).lstrip()
            if stripped.startswith(";"):
                head_normalised = stripped[1:]
                continue
            if stripped == head_normalised:
                break
            head_normalised = stripped
        head_normalised = head_normalised.upper()
        first_verb = head_normalised.split(maxsplit=1)[0].rstrip(";") if head_normalised else ""
        if first_verb in _EXECUTEMANY_REJECT_VERBS:
            raise ProgrammingError(
                f"executemany() not supported for {first_verb}; "
                "use execute() instead — transaction-control statements "
                "take no parameters and cannot be batched."
            )
        if _is_row_returning(operation) and not _is_dml_with_returning(operation):
            head_upper = operation.lstrip().upper()
            if head_upper.startswith("PRAGMA"):
                # See sync sibling: PRAGMA has per-call semantics and
                # is never meaningfully batchable; surface the
                # PRAGMA-specific guidance so the caller does not
                # wonder whether a different PRAGMA would be
                # acceptable.
                raise ProgrammingError(
                    "executemany() does not accept PRAGMA; PRAGMAs have "
                    "per-call semantics and are not batchable. Use "
                    "execute() for each PRAGMA."
                )
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
        # Hold ``op_lock`` once for the entire loop. Previously each
        # iteration called ``self.execute(...)`` which re-acquired the
        # lock, so a concurrent task on the same connection could slip
        # arbitrary statements — including ``COMMIT`` / ``ROLLBACK`` /
        # DDL — between iterations of a RETURNING / insertmanyvalues
        # batch. The sync path is already atomic because ``_run_sync``
        # holds ``_op_lock`` for the outer coroutine; this restores
        # parity.
        _, op_lock = self._connection._ensure_locks()
        async with op_lock:
            # PEP 249 §6.1.1 — clear messages under the lock; see
            # ``execute`` and ``commit`` for the under-lock-clear
            # rationale.
            del self.messages[:]
            self._check_closed()
            try:
                for params in seq_of_parameters:
                    # Re-check before each iteration so a concurrent
                    # ``cursor.close()`` landing between iterations
                    # surfaces as "Cursor is closed" rather than being
                    # observed only on the next iteration's nested execute
                    # entry (or not at all for a single-iteration
                    # remainder).
                    self._check_closed()
                    await self._execute_unlocked(operation, params)
                    self._check_closed()
                    acc.push(self)
            except BaseException:
                # Mid-batch failure leaves _rowcount at the last
                # iteration's value (misleading), so reset to
                # PEP 249's "undetermined" sentinel and clear the
                # other state fields. Mirrors the sync sibling.
                # ``_lastrowid`` is intentionally NOT reset — stdlib
                # ``sqlite3.Cursor.lastrowid`` is documented as
                # "the rowid of the last row inserted" and is NOT
                # cleared by a failed/cancelled subsequent operation.
                # A user who did ``cur.execute("INSERT ...")``, saw
                # ``cur.lastrowid``, then ran an ``executemany`` that
                # failed mid-batch should still see the prior INSERT's
                # rowid (per the cursor docstring at module top:
                # "ROLLBACK / UPDATE / DELETE / DDL do NOT clear it
                # (mirroring stdlib), but close() scrubs it").
                # PEP 249 §6.1.1 also requires messages be cleared
                # by every cursor method call; clear here so the
                # contract holds even on the BaseException re-raise
                # path.
                self._rowcount = -1
                self._rows = []
                self._description = None
                self._row_index = 0
                del self.messages[:]
                raise
            # Final guard before apply; pairs with the ``_closed``
            # check inside ``_ExecuteManyAccumulator.apply``.
            self._check_closed()
            acc.apply(self)
        return self

    def _check_result_set(self) -> None:
        if self._description is None:
            raise ProgrammingError("no results to fetch; execute a query first")

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set.

        Returns ``None`` when no more rows are available, or when no
        result set is active (DML-only / never-executed cursor).
        Stdlib parity — see sync sibling at ``cursor.py``.
        ``fetchmany`` / ``fetchall`` continue to use
        ``_check_result_set`` and raise.
        """
        del self.messages[:]
        self._check_closed()
        # Surface a loop-binding mismatch up front so a caller awaiting
        # a fetch from a different loop than the one the connection
        # was bound to gets a clear ``ProgrammingError`` rather than a
        # silent success on buffered rows. Use the non-binding helper
        # (``_check_loop_binding``) so a fresh-cursor misuse path
        # ("fetch before execute") does not lazy-bind the loop before
        # the result-set guard fires — same family of footgun the
        # other no-op-shape cursor methods (``setinputsizes`` /
        # ``setoutputsize`` / ``callproc`` / ``nextset`` / ``scroll``)
        # already adopted.
        self._connection._check_loop_binding()
        if self._description is None:
            # Match stdlib: no-result-set returns None rather than
            # raising. See sync sibling for full rationale.
            return None

        if self._row_index >= len(self._rows):
            return None

        row = self._rows[self._row_index]
        # Apply row_factory BEFORE advancing ``_row_index`` so a raise
        # inside a custom factory leaves the index unchanged. Without
        # this ordering, ``fetchmany``'s snapshot/restore at
        # ``snapshot + len(result)`` underestimates by 1 for
        # factory-raised rows — silently REPLAYING a row on the next
        # call. Mirrors the sync sibling at ``cursor.py``'s fetchone.
        if self._row_factory is not None:
            transformed: tuple[Any, ...] = self._row_factory(self, row)
            self._row_index += 1
            return transformed
        self._row_index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to ``size`` next rows of a query result.

        Returns an empty list when no more rows are available OR when
        no result set is active (DML-only / never-executed). Stdlib
        parity with ``sqlite3.Cursor.fetchmany`` matching the
        ``fetchone`` parity already in place.

        **Divergence from psycopg3**: an explicit ``size=0`` returns
        ``[]`` here (stdlib ``sqlite3`` parity). psycopg3 treats
        ``size=0`` as "use ``self.arraysize``". Pass ``None`` or
        omit ``size`` to default to ``self.arraysize``. See sync
        sibling for the rationale.
        """
        del self.messages[:]
        self._check_closed()
        # Loop-binding check; see ``fetchone`` rationale.
        self._connection._check_loop_binding()
        if self._description is None:
            # No result set active. Match stdlib by returning ``[]``.
            return []

        if size is None:
            size = self._arraysize
        elif not isinstance(size, int) or isinstance(size, bool):
            # PEP 249 §7: cursor methods must raise dbapi.Error.
            # Non-int / bool slip past stdlib's C-level int coerce
            # and produce a bare TypeError otherwise. bool is rejected
            # because ``True`` silently coerces to 1 (caller-bug trap).
            # See sync sibling at cursor.py for matching guard.
            raise ProgrammingError(f"fetchmany expects an int or None, got {type(size).__name__}")
        if size < 0:
            # Stdlib parity: ``sqlite3.Cursor.fetchmany`` documents
            # negative ``size`` as "fetch all remaining rows". Mirror
            # the sync sibling.
            return await self.fetchall()

        # Snapshot ``_row_index`` BEFORE the per-iteration ``fetchone()``
        # loop. On cancel/exception mid-loop, restore to (snapshot +
        # delivered count) so rows that were "consumed" (advanced
        # ``_row_index``) but never made it into the caller's
        # ``result`` are not silently lost. Without the restore, a
        # subsequent ``fetchmany()`` would skip those rows.
        snapshot = self._row_index
        result: list[tuple[Any, ...]] = []
        try:
            for _ in range(size):
                row = await self.fetchone()
                if row is None:
                    break
                result.append(row)
        except BaseException:
            # Restore _row_index so a retry sees the un-delivered rows.
            self._row_index = snapshot + len(result)
            raise

        return result

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result.

        Returns an empty list when the cursor has no more rows OR
        when no result set is active (DML-only / never-executed).
        Stdlib parity with ``sqlite3.Cursor.fetchall``.
        """
        del self.messages[:]
        self._check_closed()
        # Loop-binding check; see ``fetchone`` rationale.
        self._connection._check_loop_binding()
        if self._description is None:
            # No result set active. Match stdlib by returning ``[]``.
            return []

        result = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        if self._row_factory is not None:
            return [self._row_factory(self, row) for row in result]
        return result

    def drain_rows(self) -> list[tuple[Any, ...]]:
        """Transfer ownership of the row buffer to the caller.

        Returns the in-memory row list and clears it on the cursor.
        Synchronous (no ``await``) and does not honour
        ``_row_factory`` — the caller takes the raw tuples.

        Intended for adapter layers (the SQLAlchemy async adapter
        in particular) that rebuffer the rows into their own
        container immediately and would otherwise pay 2× memory
        for the duration of the transfer:

            # Naive adapter: 2× memory peak (cursor list AND deque).
            buffered = await cursor.fetchall()  # makes a copy
            self._rows = collections.deque(buffered)

            # With drain: ownership transfer, single allocation.
            self._rows = collections.deque(cursor.drain_rows())

        The cursor is unusable for fetch* afterward (the buffer is
        empty, so a follow-up ``fetchone`` returns None / ``fetchall``
        returns []) and the caller is expected to close it shortly
        afterward — the typical pattern in a SA-style ``execute /
        fetchall / close`` sequence.

        ``rowcount`` / ``lastrowid`` / ``description`` reads MUST
        come BEFORE the drain — they are independent of ``_rows``
        but reading them after a drain when downstream code might
        also have closed the cursor is a footgun. The adapter at
        ``sqlalchemy-dqlite/aio.py`` calls drain_rows last (after
        capturing the metadata fields) for that reason.
        """
        rows = self._rows
        self._rows = []
        # Position the index at the (now empty) end so any
        # subsequent fetch* call returns the no-rows result instead
        # of indexing into the empty buffer with a stale index.
        self._row_index = 0
        return rows

    async def close(self) -> None:
        """Close the cursor.

        Idempotent: a second call is a no-op.
        """
        # PEP 249 §6.1.2 messages-clear contract; see Cursor.close.
        del self.messages[:]
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
        # Mirror the sync cursor's scrub contract.
        self._row_index = 0
        # Drop the strong back-reference to the parent
        # ``AsyncConnection`` so a closed cursor the user retains
        # does not pin the connection's loop-bound ``asyncio.Lock``,
        # ``weakref.finalize`` registration, or any other
        # connection-lifecycle state past the user's intended
        # lifetime. The connection's ``_cursors`` is already a
        # ``WeakSet``; this fixes the reverse direction. See
        # ``Cursor.close`` for full rationale.
        with contextlib.suppress(
            TypeError
        ):  # pragma: no cover - AsyncConnection always supports weakref
            self._connection = weakref.proxy(self._connection)

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        """Set input sizes (no-op for dqlite).

        PEP 249 §6.1.1 names ``setinputsizes`` among the methods that
        clear the ``messages`` list; we do so even though the method
        itself does no work. ``sizes`` accepts ``Sequence[Any]`` per
        PEP 249 §6.2 — items may be a Type Object, an int, or
        ``None``.
        """
        # PEP 249 §6.1.1 — clear "prior to executing the call" so the
        # contract holds even on the cross-loop rejection path.
        del self.messages[:]
        # Validate input shape symmetric with the sync sibling so a
        # caller-side bug (e.g. passing a string) surfaces at the call
        # site rather than being silently absorbed. PEP 249 §7 keeps
        # the failure inside the ``dbapi.Error`` hierarchy.
        # ``str`` and ``bytes`` are ``Sequence`` instances at the ABC
        # level but are caller-bug shapes (passing a single string for
        # an N-element sizes list). Reject explicitly.
        if isinstance(sizes, (str, bytes, bytearray)):
            raise ProgrammingError(
                f"setinputsizes expects a sequence of size hints, got {type(sizes).__name__}"
            )
        if not isinstance(sizes, Sequence):
            # PEP 249 §6.2: ``sizes`` is "specified as a sequence".
            # Loosened from ``(list, tuple)`` to the structural
            # ``Sequence`` ABC so cross-driver callers passing a
            # ``deque`` / ``range`` / custom Sequence subclass —
            # accepted by stdlib + psycopg2 — work here too.
            raise ProgrammingError(f"setinputsizes expects a Sequence, got {type(sizes).__name__}")
        # PEP 249 §6.2 says implementations are "free to have this
        # method do nothing" — including on closed cursors. Mirror
        # the sync sibling's documented permissive-on-closed
        # behaviour: a closed-cursor cleanup helper can call
        # setinputsizes / setoutputsize without a raise. Without
        # this short-circuit, ``_check_loop_binding`` would raise
        # ``InterfaceError("Connection is closed")``, diverging from
        # the sync sibling and from the documented intent.
        if self._closed or self._connection._closed:
            return
        # Surface a loop-binding mismatch up front so callers see the
        # same ``ProgrammingError`` they'd get from ``execute`` /
        # ``fetchone``. Without this, a sync no-op on a cursor bound
        # to loop A but called from loop B silently succeeds and
        # masks the misuse until the next awaited op. Non-binding
        # helper so calling this on a fresh connection doesn't
        # lazily bind it.
        self._connection._check_loop_binding()

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Set output size (no-op for dqlite). See ``setinputsizes``."""
        del self.messages[:]
        # Validate input shape symmetric with sync sibling.
        if not isinstance(size, int) or isinstance(size, bool):
            raise ProgrammingError(f"setoutputsize expects an int, got {type(size).__name__}")
        if column is not None and (not isinstance(column, int) or isinstance(column, bool)):
            raise ProgrammingError(
                f"setoutputsize column expects an int or None, got {type(column).__name__}"
            )
        # PEP 249 §6.2 — see ``setinputsizes`` rationale.
        if self._closed or self._connection._closed:
            return
        self._connection._check_loop_binding()

    def callproc(self, procname: str, parameters: Sequence[Any] | None = None) -> NoReturn:
        """PEP 249 optional extension — not supported.

        Sync despite the cursor being async: the method raises
        unconditionally, so wrapping it in a coroutine has no value and
        would diverge from the sync siblings (``nextset`` / ``scroll``)
        and from the SQLAlchemy adapter (``sqlalchemy-dqlite``), which
        both expose these as plain methods. Annotated ``NoReturn``
        because the body always raises — symmetric with ``nextset``.
        """
        # PEP 249 §6.1.1 names ``callproc`` among the cursor methods
        # that clear ``Connection.messages`` / ``Cursor.messages``.
        # Clear before any guard so the contract holds even on the
        # closed-cursor / cross-loop / not-supported paths.
        del self.messages[:]
        # PEP 249 §6.1.2 — closed-cursor ops raise.
        self._check_closed()
        # Loop-binding check: parallel to the sync side's
        # ``_check_thread()`` for ``callproc`` / ``nextset`` /
        # ``scroll``. Without it, a call from a foreign event loop
        # silently surfaces ``NotSupportedError`` and the caller is
        # left thinking the cursor is still loop-A bound. Sibling
        # consistency with ``setinputsizes`` / ``setoutputsize``.
        self._connection._check_loop_binding()
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> NoReturn:
        """PEP 249 optional extension — not supported."""
        # PEP 249 §6.1.1 — clear before any guard.
        del self.messages[:]
        # PEP 249 §6.1.2 — closed-cursor ops raise.
        self._check_closed()
        # Loop-binding check; see ``callproc`` for rationale. Use
        # the non-binding helper so a no-op cursor method on a fresh
        # connection doesn't lazily bind the loop — a later
        # legitimate call from a different loop would otherwise fail
        # with a confusing "different event loop" diagnostic
        # referring to a loop the user did not knowingly bind.
        self._connection._check_loop_binding()
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative") -> NoReturn:
        """PEP 249 optional extension — not supported."""
        # Sibling consistency with ``nextset`` / ``callproc`` /
        # ``setinputsizes`` / ``setoutputsize``: clear ``messages`` on
        # the not-supported path so a future code path that populates
        # ``messages`` cannot leave stale entries visible after the
        # caller observed the rejection. Clear before any guard.
        del self.messages[:]
        # PEP 249 §6.1.2 — closed-cursor ops raise.
        self._check_closed()
        # Loop-binding check; see ``callproc`` for rationale. Use
        # the non-binding helper so a no-op cursor method on a fresh
        # connection doesn't lazily bind the loop — a later
        # legitimate call from a different loop would otherwise fail
        # with a confusing "different event loop" diagnostic
        # referring to a loop the user did not knowingly bind.
        self._connection._check_loop_binding()
        # PEP 249 §6.1.1 enumerates ``mode`` ∈ {"relative", "absolute"};
        # validate before NotSupportedError so a caller typo surfaces
        # as a caller-side bug. ProgrammingError stays in dbapi.Error.
        if mode not in ("relative", "absolute"):
            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', got {mode!r}")
        raise NotSupportedError("dqlite cursors are not scrollable")

    def executescript(self, sql_script: str, /) -> NoReturn:
        """stdlib ``sqlite3.Cursor``-parity stub. See sync sibling.

        Defined as a plain ``def`` (not ``async def``) so the
        unconditional raise fires on the call line. An ``async def``
        stub would defer the raise to ``await`` and a caller who
        forgot the ``await`` would observe a silent no-op with only a
        GC-time coroutine-was-never-awaited warning — defeating the
        diagnostic-leak prevention this stub family was added for.
        """
        del self.messages[:]
        self._check_closed()
        self._connection._check_loop_binding()
        raise NotSupportedError(
            "dqlite does not support stdlib sqlite3 executescript; "
            "split the script and execute each statement individually"
        )

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        # Include the parent connection's address and ``id(self)`` so
        # the repr disambiguates cursors fanned across pooled
        # connections in logs. See sync ``Cursor.__repr__``.
        address = getattr(self._connection, "_address", "?")
        return (
            f"<AsyncCursor address={address!r} rowcount={self._rowcount} {state} at 0x{id(self):x}>"
        )

    def __reduce__(self) -> NoReturn:
        # AsyncCursors hold a back-reference to a loop-bound
        # AsyncConnection; none of that survives pickling. Surface a
        # clear driver-level TypeError instead of the default pickle
        # walk's confusing internal-member message.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — async "
            "cursors hold a reference to a loop-bound driver "
            "connection; use fetchall()/fetchmany() to materialise "
            "rows before crossing a process boundary"
        )

    def __aiter__(self) -> Self:
        # Surface a loop-mismatch at the ``async for cursor:`` site
        # rather than one await deeper inside ``__anext__``'s
        # ``fetchone``. Use the loop-only variant
        # (``_check_loop_only``) so a closed connection / closed
        # cursor does NOT raise here — sync ``Cursor.__iter__`` is
        # bare ``return self`` per PEP 234 + project pin
        # (``test_pep249_misc_pins.py``); the async sibling matches
        # so ``aiter(cur) is cur`` works on closed cursors too.
        # The closed-state diagnostic is deferred to the first
        # ``__anext__`` / ``fetchone``, matching the synchronous
        # pin's documented design.
        self._connection._check_loop_only()
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
