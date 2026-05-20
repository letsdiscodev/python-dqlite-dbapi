"""Async cursor implementation for dqlite."""

import asyncio
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
    _validate_executemany_seq_shape,
)
from dqlitedbapi.exceptions import (
    DataError,
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
)
from dqlitedbapi.types import RowFactory, _Description
from dqlitewire import ValueType

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
        "_completed_iterations",
        "_connection",
        "_description",
        "_executing_task",
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
        # Per-cursor task token used to reject concurrent execute()
        # calls from different tasks. ``op_lock`` serialises the wire
        # but not the cursor's per-execute state mutations.
        self._executing_task: asyncio.Task[Any] | None = None
        # See sync sibling for full docs. Tracks the count of
        # executemany iterations that completed successfully; preserved
        # across cancel for idempotent-compensation observability.
        self._completed_iterations: int = 0
        # Inherit parent connection's default row_factory (stdlib
        # parity). ``isinstance`` admits real AsyncConnection instances
        # and user subclasses (a common cross-cutting pattern) while
        # MagicMock-typed test fakes still fall through to ``None``.
        # The import is deferred to call time to break the cursor →
        # connection import cycle (``AsyncConnection`` only appears in
        # ``TYPE_CHECKING`` at module scope).
        from dqlitedbapi.aio.connection import AsyncConnection as _AsyncConnection

        self._row_factory: RowFactory | None = (
            getattr(connection, "_row_factory", None)
            if isinstance(connection, _AsyncConnection)
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
    def completed_iterations(self) -> int:
        """Count of executemany() iterations that completed
        successfully on the most recent call. See sync sibling
        ``Cursor.completed_iterations`` for full docs."""
        return self._completed_iterations

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
        # PEP 249 §6.4 ``messages`` clear-on-entry; mirrors the sync
        # sibling and the connection-side setters.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        # PEP 249 §6.1.2: any state-mutating method on a closed cursor
        # must raise an ``Error`` subclass. Apply the closed-state
        # guard FIRST so a bool/int validation error doesn't shadow
        # the closed-cursor error. Mirrors the sync sibling
        # ``Cursor.arraysize`` setter.
        self._check_closed()
        # Loop-binding affinity contract — a state-mutating setter on
        # a Connection-allocated cursor must be invoked from the
        # owning loop. Without this, a foreign-loop
        # ``cur.arraysize = 1`` mid-batch silently swaps the creator
        # loop's next ``fetchmany`` size. Sibling sync setter calls
        # ``_check_thread()`` for the same reason.
        self._connection._check_loop_binding()
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
    def row_factory(self) -> RowFactory | None:
        """stdlib ``sqlite3.Cursor.row_factory`` parity hook. See
        sync sibling ``Cursor.row_factory`` for full docs."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        # PEP 249 §6.4 ``messages`` clear-on-entry; see ``arraysize.setter``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        self._check_closed()
        # Loop-binding affinity contract — see ``arraysize.setter``
        # for rationale.
        self._connection._check_loop_binding()
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
        # See the sync sibling: reset ``_completed_iterations`` here
        # too so the property's documented "0 after a single-row
        # execute" contract holds across an executemany → execute
        # transition. ``executemany`` already calls
        # ``_reset_execute_state``, so the explicit reset there is
        # redundant once the helper takes responsibility.
        self._completed_iterations = 0

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
            # Post-await close-race guard: a sibling task may have
            # called ``self.close()`` while we were parked on the
            # wire. ``close()`` is synchronous and sets ``_closed =
            # True`` GIL-atomically, plus clears ``_rows`` /
            # ``_description``; without this re-check, the result-
            # population below would re-populate state onto the now-
            # closed cursor. Drop the result silently (close is a no-
            # error termination — raising into the awaiter's frame
            # would surprise a caller that just used a try/except for
            # CancelledError). Mirrors the same discipline already
            # applied in ``_ExecuteManyAccumulator.apply``'s post-
            # await re-check arm.
            if self._closed:
                return
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
                    type_codes: list[int | None] = [None] * len(columns)
                elif len(column_types) != len(columns):
                    raise DataError(
                        f"Wire response has {len(columns)} columns but "
                        f"{len(column_types)} type codes"
                    )
                else:
                    # Map ValueType.NULL → None to satisfy PEP 249
                    # §6.1.2 ("type_code must compare equal to one of
                    # Type Objects"). See sync sibling rationale.
                    type_codes = [None if c == ValueType.NULL else int(c) for c in column_types]
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
            # Same post-await close-race guard as the query branch.
            if self._closed:
                return
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

        Concurrency: a single ``AsyncCursor`` is a single-task
        primitive. Two tasks issuing ``await cur.execute(...)``
        concurrently on the same cursor would otherwise silently
        clobber each other's per-execute state (``_description``,
        ``_rows``, ``_rowcount``) — the connection's ``op_lock``
        serialises the wire but not the cursor instance. asyncpg
        rejects the same shape with ``InterfaceError("cursor is
        already executing")``; this driver matches.
        """
        # PEP 249 §6.1.2: ``messages`` is cleared by every standard
        # cursor method before the call runs.
        del self.messages[:]
        # Fast-path guard outside the lock so we fail quickly on an
        # already-closed cursor without taking the lock.
        self._check_closed()
        # Scrub per-execute state (description / rowcount / rows /
        # row_index) BEFORE the non-str and cross-task slot rejects so
        # a rejected ``execute`` lands at the stdlib "no result set"
        # baseline rather than reporting the prior query's shape. The
        # closed guard above still precedes the reset so a caller
        # executing on a closed cursor sees the sharp
        # ``InterfaceError("Cursor is closed")`` without a
        # state-clobber side effect. ``_reset_execute_state``
        # deliberately does NOT touch ``_lastrowid`` or
        # ``_executing_task``, so the preserve-across-rejection
        # contract for lastrowid is unaffected and a foreign task's
        # slot is left intact. Mirrors the ``executemany`` sibling.
        self._reset_execute_state()
        # PEP 249 §7: errors raised by the module subclass ``Error``.
        # A non-str ``operation`` would later raise bare ``AttributeError``
        # (``None.lstrip``) or ``TypeError`` (``bytes.lstrip("﻿")``)
        # from ``_strip_leading_comments`` inside ``_classify_caller_sql``,
        # escaping the dbapi exception hierarchy. Symmetric with the
        # sync sibling and with the ``executemany`` ``seq_of_parameters=None``
        # guard.
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )
        # Reject concurrent execute on the same cursor. ``op_lock``
        # below serialises the wire calls, but the cursor's
        # per-execute state is mutated outside that lock (the
        # ``_reset_execute_state`` call AND the result population in
        # ``_execute_unlocked``) — two concurrent execute() calls on
        # the same cursor object see different result-set state at
        # different times. Use a per-cursor task token; reject any
        # concurrent entry from a foreign task.
        cur_task = asyncio.current_task()
        if self._executing_task is not None and self._executing_task is not cur_task:
            raise InterfaceError(
                f"cursor is already executing in another task (id={id(self)}); "
                "use one cursor per task"
            )
        # Set the slot INSIDE the try/finally so a KeyboardInterrupt /
        # SystemExit delivered at the bytecode boundary between the
        # STORE_ATTR and the SETUP_FINALLY cannot leave the slot pinned
        # to a now-completed task. Mirrors the executemany sibling.
        try:
            self._executing_task = cur_task

            # Pre-flight classification of caller-supplied SQL — empty /
            # multi-statement / wrong ``?``-count. Mirrors the sync
            # sibling at cursor.py. See ``_classify_caller_sql`` docstring.
            _classify_caller_sql(operation, parameters)

            _, op_lock = self._connection._ensure_locks()
            async with op_lock:
                del self.messages[:]
                self._check_closed()
                await self._execute_unlocked(operation, parameters)
        finally:
            # Clear unconditionally to close the bytecode-tight signal
            # window between the read and write of a guarded clear: a
            # BaseException between ``is`` and ``STORE_ATTR`` would
            # otherwise leave the slot pinned to a now-completed task.
            # Safe because ``row_factory`` runs only in fetch*; execute*
            # never re-enters the same cursor's execute path from a
            # row callback. The cross-task rejection above remains the
            # primary guard against concurrent execute on one cursor.
            self._executing_task = None

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
        # Scrub per-execute state (description / rowcount / rows /
        # row_index) BEFORE every rejection guard — input-validation
        # (None seq / bad outer shape / non-str operation / cross-task
        # slot) AND SQL-content (verb-reject / row-returning-reject /
        # PRAGMA) — so a rejected ``executemany`` lands at the stdlib
        # "no result set" baseline rather than reporting the prior
        # query's shape. ``_reset_execute_state`` deliberately does
        # NOT touch ``_lastrowid``, so the preserve-across-rejection
        # contract for lastrowid is unaffected. Also zeroes
        # ``_completed_iterations`` so an empty ``seq_of_parameters``
        # ends with the same shape as empty ``execute``; the counter
        # is preserved across the BaseException re-raise so callers
        # can observe how many iterations committed before the
        # cancel / failure. Runs BEFORE the cross-task slot check so
        # the slot state itself is untouched (the reset does not
        # touch ``_executing_task``).
        self._reset_execute_state()
        # PEP 249 §7: errors raised by the module subclass ``Error``.
        # ``seq_of_parameters=None`` would later leak a bare ``TypeError``
        # ("'NoneType' object is not iterable") from the iteration site
        # below, escaping the dbapi exception hierarchy. ``None`` for
        # the outer iterable has no defensible "no params" reading
        # (unlike ``execute(sql, None)``); mirror the project's
        # existing strict input-validation discipline (str/bytes/Mapping/
        # set rejection in ``_reject_non_sequence_params``) and surface
        # ``ProgrammingError`` up front. Same treatment in the sync
        # sibling.
        if seq_of_parameters is None:
            raise ProgrammingError(
                "executemany() seq_of_parameters must be a sequence/iterable, not None",
                code=None,
            )
        # Reject outer shapes that would silently iterate over keys
        # (dict) / characters (str / bytes / bytearray / memoryview) or
        # iterate in non-deterministic order (set / frozenset). Shared
        # with the sync sibling and with the ``AsyncConnection.executemany``
        # shortcut so the four entry points share one diagnostic and one
        # accept/reject contract.
        _validate_executemany_seq_shape(seq_of_parameters)
        # PEP 249 §7: surface non-str ``operation`` as a ``dbapi.Error``
        # subclass up front so cross-driver ``except dbapi.Error:`` catches
        # the misuse. Mirrors the canonical sibling guard on
        # ``AsyncCursor.execute`` and the sync ``Cursor.executemany``.
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )
        # Reject concurrent execute/executemany on the same cursor
        # — see ``execute`` for full rationale. The slot-state check
        # observes the existing slot BEFORE the slot is set; the
        # set itself moves inside the try/finally below so a
        # validation-rejected executemany clears the slot on raise
        # (sibling ``execute`` already follows this pattern).
        cur_task = asyncio.current_task()
        if self._executing_task is not None and self._executing_task is not cur_task:
            raise InterfaceError(
                f"cursor is already executing in another task (id={id(self)}); "
                "use one cursor per task"
            )
        # Set the slot INSIDE the try/finally so any non-success exit
        # path (validation reject, mid-loop raise, cancel) clears it.
        # Previously the slot was set BEFORE the verb-reject /
        # PRAGMA-reject / row-returning-reject checks; a validation-
        # rejected executemany pinned ``_executing_task`` to a
        # completed task, then a cross-task ``cur.execute(...)``
        # observed the stale slot and raised
        # ``InterfaceError("cursor is already executing in another task")``
        # on a cursor that was NOT actually executing.
        try:
            self._executing_task = cur_task
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

            # Per-execute state was scrubbed BEFORE the reject guards
            # above so a rejected batch lands at the stdlib baseline.
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
                        self._completed_iterations += 1
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
                    # ``_completed_iterations`` is intentionally PRESERVED
                    # — it's the observability signal for "how many
                    # iterations committed before the failure"; callers
                    # reading it after cancel get the count for
                    # idempotent compensation. Mirrors the sync sibling.
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
        finally:
            # Clear unconditionally — see ``execute`` finally for the
            # rationale (closes the bytecode-tight signal window
            # between the ``is`` read and the ``STORE_ATTR`` write).
            self._executing_task = None
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

        return self._next_row_unlocked()

    def _next_row_unlocked(self) -> tuple[Any, ...] | None:
        """Advance one row + apply ``row_factory`` without clearing
        ``messages`` or re-running guards.

        Mirrors the sync sibling. Used by both ``fetchone`` (after ITS
        prelude clear/guards) and ``fetchmany`` (after ITS single
        prelude clear/guards). PEP 249 §6.1.1 requires the messages
        clear once per top-level method invocation, NOT once per
        inner row delivery.
        """
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        # Apply row_factory BEFORE advancing ``_row_index`` so a raise
        # inside a custom factory leaves the index unchanged. Without
        # this ordering, ``fetchmany``'s snapshot/restore at
        # ``snapshot + len(result)`` underestimates by 1 for
        # factory-raised rows — silently REPLAYING a row on the next
        # call.
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
        parity with ``sqlite3.Cursor.fetchmany`` for the "no result
        set" case matching the ``fetchone`` parity already in place.

        **``size=0`` divergence (cross-driver matrix)**: dqlite
        returns ``[]`` deterministically. This differs from stdlib
        ``sqlite3`` (whose behaviour for ``fetchmany(0)`` is
        version-dependent — some Python/sqlite releases drain the
        result set, others return ``[]``) and from psycopg3 (which
        treats ``0`` as the sentinel "use ``self.arraysize``").
        Cross-driver code should pass an explicit positive size,
        use ``None`` / omit ``size`` to default to ``self.arraysize``,
        or rely on ``fetchall()`` to drain. See sync sibling for the
        full rationale.
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

        # Snapshot ``_row_index`` BEFORE the loop. On cancel/exception
        # mid-loop, restore to (snapshot + delivered count) so rows
        # that were "consumed" (advanced ``_row_index``) but never made
        # it into the caller's ``result`` are not silently lost.
        # Without the restore, a subsequent ``fetchmany()`` would skip
        # those rows.
        # Use the ``_next_row_unlocked`` helper so the per-row path
        # does not re-clear ``messages`` (PEP 249 §6.1.1: once per
        # top-level call, not once per row) or re-run the closed /
        # loop-binding guards already validated in this method's
        # prelude.
        snapshot = self._row_index
        result: list[tuple[Any, ...]] = []
        try:
            for _ in range(size):
                row = self._next_row_unlocked()
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
        if self._row_factory is not None:
            # Apply factory BEFORE advancing ``_row_index``. Symmetric
            # with the sync sibling and with ``fetchone`` / ``fetchmany``
            # discipline — a raise inside a custom factory leaves the
            # cursor index unchanged so the next fetchone returns the
            # same row.
            transformed = [self._row_factory(self, row) for row in result]
            self._row_index = len(self._rows)
            return transformed
        self._row_index = len(self._rows)
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

    def close(self) -> None:
        """Close the cursor.

        Idempotent and **synchronous by design**. The body has zero
        await statements (every operation is a GIL-atomic attribute
        write plus the weakref-proxy swap); making it ``async def``
        would invite a forgot-``await`` footgun where ``cur.close()``
        silently produced a discarded coroutine and the cursor was
        left undrained, with only a GC-time
        ``RuntimeWarning("coroutine was never awaited")`` pointing at
        asyncio internals rather than at dqlite. Mirrors stdlib
        ``sqlite3.Cursor.close`` and the project's
        ``executescript`` / ``interrupt`` / ``backup`` / ``tpc_*``
        family (sync ``def`` stubs that surface forgot-call as
        immediate ``NotSupportedError`` rather than a discarded
        coroutine).

        Scrubs ``description`` / ``rowcount`` / ``lastrowid`` /
        ``_rows`` / ``_row_index`` symmetrically with the sync sibling
        ``Cursor.close`` (see that docstring for the full
        "post-close state" rationale).

        **``arraysize`` is deliberately NOT scrubbed**: it is a
        caller-set configuration *hint* (PEP 249 §6.1.2 default ``1``;
        used by ``fetchmany()`` when ``size`` is omitted), not
        result-set state. Stdlib ``sqlite3.Cursor`` and psycopg2 both
        retain ``arraysize`` across ``close()``; this driver matches
        that parity. ``arraysize`` is therefore the single PEP 249
        §6.1.2 attribute outside the scrub set above — by design.

        Cross-task safety: ``_closed = True`` is set FIRST (GIL-atomic
        write) so a sibling task whose ``_execute_unlocked`` is parked
        on the wire await observes the flag-flip on resume. The
        executor's post-await ``if self._closed: return`` short-circuit
        prevents the wire response from re-populating ``_rows`` /
        ``_description`` onto a closed cursor.
        """
        # PEP 249 §6.1.2 messages-clear contract; see Cursor.close.
        del self.messages[:]
        if self._closed:
            return
        # Set the flag FIRST so a sibling-task ``_execute_unlocked``
        # that is parked on the wire await observes it on resume and
        # short-circuits before repopulating state.
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

    def setinputsizes(self, sizes: Sequence[Any] | None) -> None:
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
        # PEP 249 §6.2 says implementations are "free to have this
        # method do nothing" — including on closed cursors. The
        # closed short-circuit runs BEFORE the input-shape validators
        # so closed-state behaviour is independent of argument shape.
        # Mirror the sync sibling's documented permissive-on-closed
        # contract: a closed-cursor cleanup helper can call
        # setinputsizes / setoutputsize without a raise regardless of
        # argument shape. Without this short-circuit,
        # ``_check_loop_binding`` would raise
        # ``InterfaceError("Connection is closed")``, diverging from
        # the sync sibling and from the documented intent.
        if self._closed or self._connection._closed:
            return
        # Affinity-before-shape on the open-cursor path: surface a
        # loop-binding mismatch up front so callers see the same
        # ``ProgrammingError`` they'd get from ``execute`` /
        # ``fetchone``. Without this, a sync no-op on a cursor bound
        # to loop A but called from loop B silently succeeds and
        # masks the misuse until the next awaited op. Non-binding
        # helper so calling this on a fresh connection doesn't
        # lazily bind it. Mirrors the sync sibling's affinity-before-
        # shape ordering and the ``nextset`` / ``scroll`` /
        # ``executescript`` / ``callproc`` ordering convention.
        self._connection._check_loop_binding()
        # PEP 249 §6.2 permits no-op implementations. Stdlib
        # ``sqlite3``, aiosqlite, psycopg, and asyncpg all accept
        # ``None`` silently. Symmetric with the sync sibling: treat
        # ``None`` as a no-op for cross-driver portability while
        # keeping the strict rejection below for invalid types.
        if sizes is None:
            return
        # Validate input shape symmetric with the sync sibling so a
        # caller-side bug (e.g. passing a string) surfaces at the call
        # site rather than being silently absorbed. PEP 249 §7 keeps
        # the failure inside the ``dbapi.Error`` hierarchy.
        if isinstance(sizes, (str, bytes, bytearray, memoryview)):
            # ``memoryview`` satisfies ``collections.abc.Sequence`` so
            # it would slip past the explicit-rejection arm and reach
            # the ABC arm below. Quartet-rejection keeps this validator
            # aligned with the sibling ``_reject_non_sequence_params``
            # (str/bytes/bytearray/memoryview).
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

    def setoutputsize(self, size: int | None, column: int | None = None) -> None:
        """Set output size (no-op for dqlite). See ``setinputsizes``."""
        del self.messages[:]
        # PEP 249 §6.2 — closed short-circuit before validators. See
        # ``setinputsizes`` rationale.
        if self._closed or self._connection._closed:
            return
        # Affinity-before-shape on the open-cursor path; see
        # ``setinputsizes`` for the full rationale.
        self._connection._check_loop_binding()
        # PEP 249 §6.2 permits no-op implementations. Stdlib
        # ``sqlite3``, aiosqlite, psycopg, and asyncpg all accept
        # ``None`` silently. Symmetric with the sync sibling and with
        # ``setinputsizes``: treat ``None`` as a no-op for cross-driver
        # portability while keeping the strict rejection below for
        # invalid types.
        if size is None:
            return
        # Validate input shape symmetric with sync sibling.
        if not isinstance(size, int) or isinstance(size, bool):
            raise ProgrammingError(f"setoutputsize expects an int, got {type(size).__name__}")
        if column is not None and (not isinstance(column, int) or isinstance(column, bool)):
            raise ProgrammingError(
                f"setoutputsize column expects an int or None, got {type(column).__name__}"
            )

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
        # PEP 249 §6.1.1 documents ``value`` as an integer offset.
        # Validate the value-type alongside the mode enumeration so the
        # "surfaces as a caller-side bug" argument applies symmetrically
        # to both parameters. ``bool`` is-a ``int`` in Python; explicit
        # rejection matches the project standard from
        # ``arraysize.setter``.
        if not isinstance(value, int) or isinstance(value, bool):
            raise ProgrammingError(
                f"scroll value must be an integer offset, got {type(value).__name__}"
            )
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

    def _check_parent_loop_only(self) -> None:
        # Shared helper used by ``__aiter__`` and ``__aenter__``.
        #
        # ``Cursor.close()`` swaps ``self._connection`` for a
        # ``weakref.proxy``. Once the parent ``AsyncConnection`` is
        # GC'd, attribute access through the proxy raises
        # ``ReferenceError`` — outside the PEP 249 ``Error``
        # hierarchy. Translate to ``InterfaceError`` so cross-driver
        # code wrapping ``async with cur:`` / ``async for cur:`` in
        # ``except dbapi.Error:`` continues to match. Mirrors the
        # ``connection`` property's discipline.
        try:
            self._connection._check_loop_only()
        except ReferenceError as e:
            raise InterfaceError(
                f"Cursor's parent AsyncConnection has been garbage-collected (id={id(self)})"
            ) from e

    def __aiter__(self) -> Self:
        # PEP 249 §6.4 messages-clear contract: every public cursor
        # method clears ``messages`` "prior to executing the call".
        # Symmetric with the sync sibling ``Cursor.__iter__``; sibling
        # no-op cursor methods (``nextset`` / ``callproc`` /
        # ``scroll`` / ``setinputsizes`` / ``setoutputsize``) all
        # clear first. ``__anext__`` itself dispatches to
        # ``fetchone`` (which clears at its own entry, including on
        # the terminating call that returns None and surfaces
        # ``StopAsyncIteration``), so the iter-protocol's clear
        # obligation is satisfied at every step. The clear here is
        # for the ``aiter(cur)`` entry point itself — generic
        # consumer code that calls ``aiter(cur)`` and then never
        # advances would otherwise observe stale messages from a
        # prior operation on the cursor.
        del self.messages[:]
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
        #
        # Note: the sync ``Cursor.__iter__`` does NOT fail fast on
        # cross-thread misuse — it defers to the first ``__next__``,
        # matching stdlib ``sqlite3.Cursor.__iter__``. The async
        # divergence here is deliberate because ``async for`` is the
        # only common idiom that crosses event loops; the lazy
        # loop-bind contract makes the iter-time check structurally
        # only available on the async side. See the matching note on
        # ``Cursor.__iter__`` in ``../cursor.py``.
        self._check_parent_loop_only()
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> Self:
        # PEP 249 §6.4 messages-clear contract — unconditional,
        # mirroring the sibling __aiter__ above which also clears
        # regardless of _closed state. Every secondary entry point
        # in the cursor surface (nextset / callproc / scroll /
        # setinputsizes / setoutputsize / __iter__ / __aiter__ /
        # __enter__) clears unconditionally; the closed-cursor case
        # is admitted because a future driver path that appends to
        # messages from a cross-loop / cross-thread background
        # producer (e.g., a deferred warning enqueued from
        # ``_invalidate``) must not be observed by ``async with cur:``
        # on a closed cursor.
        del self.messages[:]
        # Surface loop-binding mismatches up front (mirroring
        # ``__aiter__``), so a cursor created on loop A and entered
        # via ``async with cur:`` on loop B raises at the ``with``
        # line rather than silently delaying the diagnostic to the
        # first body await. Non-binding so a never-used cursor on a
        # fresh connection doesn't lazy-bind from ``__aenter__``.
        # ``_check_parent_loop_only`` translates ``ReferenceError``
        # from a GC'd proxy parent into ``InterfaceError`` (PEP 249).
        self._check_parent_loop_only()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # ``close`` is now sync (see docstring) — no await needed.
        self.close()
