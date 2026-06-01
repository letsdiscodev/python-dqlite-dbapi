"""Async cursor implementation for dqlite."""

import asyncio
import contextlib
import weakref
from collections.abc import Coroutine, Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, NoReturn, Self

from dqlitedbapi._constants import _is_int_not_bool
from dqlitedbapi.cursor import (
    _CONVERT_ROWS_YIELD_EVERY,
    _EXECUTEMANY_REJECT_VERBS,
    _LARGE_RESULT_ROW_THRESHOLD,
    _call_client,
    _classify_caller_sql,
    _convert_params_async,
    _convert_rows_async,
    _ExecuteManyAccumulator,
    _is_commit_statement,
    _is_dml_rowcount_meaningful,
    _is_dml_with_returning,
    _is_insert_or_replace,
    _is_pragma,
    _is_row_returning,
    _strip_leading_comments,
    _strip_sql_noise,
    _to_signed_int64,
    _validate_caller_param_shape,
    _validate_executemany_seq_shape,
)
from dqlitedbapi.exceptions import (
    AMBIGUOUS_COMMIT_CODES,
    AmbiguousCommitError,
    DataError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from dqlitedbapi.types import UNKNOWN as _UNKNOWN_TYPE
from dqlitedbapi.types import RowFactory, _DBAPIType, _Description
from dqlitewire import ValueType, sanitize_for_log

if TYPE_CHECKING:
    from dqlitedbapi.aio.connection import AsyncConnection


__all__ = ["AsyncCursor"]


# ``__anext__`` yield cadence: fetchone is sync for buffered rows, so without
# this an ``async for`` over a large result would monopolise the loop.
_ANEXT_YIELD_EVERY: Final[int] = 512


async def _resolve_null_rescue_type_codes(
    column_types: Sequence[int],
    row_types: Sequence[Sequence[int]],
) -> list[int | _DBAPIType]:
    """Resolve ``description`` type codes; for NULL-first-row columns scan later
    rows for a non-NULL tag, falling back to ``UNKNOWN`` only if all rows NULL."""
    # PEP 249 §6.1.2: emit a real Type Object, not None. Yields every
    # _CONVERT_ROWS_YIELD_EVERY steps on large results (cf. _convert_rows_async).
    yield_enabled = len(row_types) >= _LARGE_RESULT_ROW_THRESHOLD
    type_codes: list[int | _DBAPIType] = []
    scanned = 0
    for col_idx, c in enumerate(column_types):
        if c != ValueType.NULL:
            type_codes.append(int(c))
            continue
        resolved: int | _DBAPIType = _UNKNOWN_TYPE
        for j in range(1, len(row_types)):
            if col_idx < len(row_types[j]):
                candidate = row_types[j][col_idx]
                if candidate != ValueType.NULL:
                    resolved = int(candidate)
                    break
            if yield_enabled:
                scanned += 1
                if scanned % _CONVERT_ROWS_YIELD_EVERY == 0:
                    await asyncio.sleep(0)
        type_codes.append(resolved)
    return type_codes


class AsyncCursor:
    """Async database cursor."""

    # ``__weakref__`` lets ``AsyncConnection._cursors`` (a WeakSet) hold a
    # reference for the close-cascade.
    __slots__ = (
        "__weakref__",
        "_aiter_yield_counter",
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
        # ``__anext__`` yield counter; reset per execute in _reset_execute_state.
        self._aiter_yield_counter: int = 0
        # Task token to reject concurrent execute() from different tasks; op_lock
        # serialises the wire but not the cursor's per-execute state mutations.
        self._executing_task: asyncio.Task[Any] | None = None
        # Executemany iterations completed; preserved across cancel for
        # idempotent-compensation observability.
        self._completed_iterations: int = 0
        # Deferred import breaks the cursor -> connection import cycle. isinstance
        # admits real AsyncConnections + subclasses; MagicMock fakes fall to None.
        from dqlitedbapi.aio.connection import AsyncConnection as _AsyncConnection

        self._row_factory: RowFactory | None = (
            getattr(connection, "_row_factory", None)
            if isinstance(connection, _AsyncConnection)
            else None
        )
        self.messages: list[tuple[type[Exception], Exception]] = []

    @property
    def connection(self) -> "AsyncConnection":
        """The AsyncConnection this cursor was created from (read-only)."""
        # close() swaps _connection for a weakref.proxy; a GC'd parent raises
        # ReferenceError (and partial-init/mock parents AttributeError), both
        # outside the PEP 249 hierarchy. Re-raise as InterfaceError so
        # ``except dbapi.Error:`` matches.
        try:
            _ = self._connection.address
        except ReferenceError as e:
            raise InterfaceError(
                "Cursor's parent AsyncConnection has been garbage-collected"
            ) from e
        except AttributeError as e:
            raise InterfaceError(
                f"Cursor's parent AsyncConnection unavailable: {type(e).__name__}: {e}"
            ) from e
        return self._connection

    @property
    def description(self) -> _Description:
        """7-tuples (name, type_code, None*5) for the last query.

        type_code is the wire ValueType int; other fields None (dqlite omits).
        Mixed-type columns flatten to the first non-NULL row's tag.
        """
        return self._description

    @property
    def completed_iterations(self) -> int:
        """Count of executemany() iterations that completed on the last call."""
        return self._completed_iterations

    @property
    def rowcount(self) -> int:
        """Rows affected (DML) or, for SELECT, rows produced (dqlite buffers the
        full result, unlike stdlib's -1); -1 if unknown/inapplicable.
        """
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        """ROWID of this cursor's most-recent successful INSERT, else None.

        Cursor-scoped; survives ROLLBACK/UPDATE/DELETE/DDL and close().
        NOT updated for ``INSERT ... RETURNING`` (dqlite's wire omits
        last_insert_id on row-returning responses) — read it from the row.
        """
        return self._lastrowid

    @property
    def rownumber(self) -> int | None:
        """0-based index of the next row; None if no result set is active.

        Runs _check_loop_only() so a foreign-loop reader can't observe a
        mid-fetch index. Closed cursors return None (ambiguous with "no result
        set" since close() scrubs _description) — gate on _closed to distinguish.
        """
        # Closed short-circuit BEFORE the loop check so the closed->None contract
        # holds across loops (loop binding must not weaponise that path).
        if self._closed:
            return None
        conn = getattr(self, "_connection", None)
        try:
            check = getattr(conn, "_check_loop_only", None)
        except ReferenceError:
            check = None
        if check is not None:
            check()
        if self._description is None:
            return None
        return self._row_index

    @property
    def arraysize(self) -> int:
        """Number of rows to fetch at a time with fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        # Closed guard FIRST so a validation error doesn't shadow the closed error.
        self._check_closed()
        # Loop-binding guard: a foreign-loop setter would otherwise silently swap
        # the creator loop's next fetchmany size.
        self._connection._check_loop_binding()
        # Reject bools (int subclass): ``arraysize = True`` coercing to 1 is a trap.
        if not _is_int_not_bool(value):
            raise ProgrammingError(
                f"arraysize must be a non-negative int, got {type(value).__name__}"
            )
        if value < 0:
            raise ProgrammingError(f"arraysize must be non-negative, got {value}")
        self._arraysize = value

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` has been called."""
        return self._closed

    @property
    def row_factory(self) -> RowFactory | None:
        """stdlib ``sqlite3.Cursor.row_factory`` parity hook."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        self._check_closed()
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

        Runs OUTSIDE op_lock (the lock serialises the wire, not in-memory
        fields). _lastrowid is deliberately NOT cleared (survives across execute).
        """
        self._description = None
        self._rows = []
        self._row_index = 0
        self._rowcount = -1
        self._completed_iterations = 0
        self._aiter_yield_counter = 0

    async def _execute_unlocked(
        self, operation: str, parameters: Sequence[Any] | None = None
    ) -> None:
        """Body of a single ``execute`` — caller already holds ``op_lock``.

        Factored out so executemany holds the lock once across all iterations
        (the per-iteration drop let a concurrent task slip COMMIT/ROLLBACK/DDL
        between iterations). Caller clears messages, holds op_lock, pre/post
        _check_closed(), and resets state on the first iteration.
        """
        is_query = _is_row_returning(operation)
        params = await _convert_params_async(parameters)
        self._check_closed()
        conn = await self._connection._ensure_connection()
        # _ensure_connection awaits, so close() can race; re-check before the wire.
        self._check_closed()
        if is_query:
            columns, column_types, row_types, rows = await _call_client(
                conn.query_raw_typed(operation, params)
            )
            # Post-await close-race guard: a sibling close() may have flipped
            # _closed while we were parked on the wire. Drop the result silently
            # and re-scrub the introspection fields (not gated by _check_closed,
            # so a bare return would leave the PRIOR query's values visible).
            if self._closed:
                self._description = None
                self._rows = []
                self._row_index = 0
                self._rowcount = -1
                return
            if not columns:
                # PRAGMA write-form routes here but produces no columns; match
                # stdlib's description=None / rowcount=-1 for non-result stmts.
                self._description = None
                self._rows = []
                self._row_index = 0
                self._rowcount = -1
                return
            else:
                # PEP 249 §6.1.2: type_code must compare equal to a Type Object.
                # Empty result set has unrecoverable type info -> UNKNOWN sentinel
                # (a real Type Object, not None). Non-empty but short -> DataError.
                if len(column_types) == 0 and len(rows) == 0:
                    type_codes: list[int | _DBAPIType] = [_UNKNOWN_TYPE] * len(columns)
                elif len(column_types) != len(columns):
                    raise DataError(
                        f"Wire response has {len(columns)} columns but "
                        f"{len(column_types)} type codes"
                    )
                else:
                    type_codes = await _resolve_null_rescue_type_codes(column_types, row_types)
                self._description = tuple(
                    (name, type_codes[i], None, None, None, None, None)
                    for i, name in enumerate(columns)
                )
            self._rows = await _convert_rows_async(rows, row_types, column_types)
            self._row_index = 0
            # dqlite returns the buffered row count (not stdlib's -1): the wire
            # buffers up front and the RETURNING path relies on this for SA's
            # insertmanyvalues. PRAGMA reads are the exception — stdlib reports
            # -1 for ALL PRAGMA, so match that rather than len.
            self._rowcount = -1 if _is_pragma(operation) else len(rows)
        else:
            try:
                last_id, affected = await _call_client(conn.execute(operation, params))
            except OperationalError as e:
                # Mirror Connection.commit() and the sync cursor: an explicit COMMIT that
                # loses leadership after the entry was submitted leaves the write in doubt.
                # A not-leader rejection is a clean pre-apply failure and stays OperationalError.
                if _is_commit_statement(operation) and e.code in AMBIGUOUS_COMMIT_CODES:
                    raise AmbiguousCommitError(
                        "ambiguous commit: leadership lost during COMMIT; "
                        "the write may or may not have been persisted. "
                        f"Original: {e}",
                        code=e.code,
                        raw_message=getattr(e, "raw_message", None),
                    ) from e
                raise
            # Post-await close-race guard (see query branch). _lastrowid is
            # PRESERVED: stdlib persists it across a close-during-DML boundary.
            if self._closed:
                self._description = None
                self._rows = []
                self._row_index = 0
                self._rowcount = -1
                return
            # stdlib parity: lastrowid only updates on INSERT / REPLACE.
            if _is_insert_or_replace(operation):
                self._lastrowid = _to_signed_int64(last_id)
            if _is_dml_rowcount_meaningful(operation):
                self._rowcount = _to_signed_int64(affected)
            else:
                self._rowcount = -1
            self._description = None
            self._rows = []
            # Leave the cursor at row 0 so a subsequent SELECT iterator is clean.
            self._row_index = 0

    async def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> Self:
        """Execute a query or command; returns ``self`` for chaining.

        A single AsyncCursor is single-task: concurrent execute() from another
        task is rejected (op_lock serialises the wire but not per-execute state).
        """
        del self.messages[:]
        # Fast-path guard outside the lock to fail quickly on a closed cursor.
        self._check_closed()
        # Input-validation rejections FIRST: stdlib preserves prior cursor state
        # on the non-str path; the reset below handles prepare-stage rejections.
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )
        # Reject concurrent execute on the same cursor (per-execute state is
        # mutated outside op_lock, so two callers would clobber each other).
        cur_task = asyncio.current_task()
        if self._executing_task is not None and self._executing_task is not cur_task:
            raise InterfaceError(
                f"cursor is already executing in another task (id={id(self)}); "
                "use one cursor per task"
            )
        # Caller-shape rejection BEFORE the reset so the prior result set survives
        # (stdlib parity) for a retry-with-coerce idiom inspecting cur.description.
        _validate_caller_param_shape(parameters)
        # Scrub per-execute state so a rejected execute lands at the stdlib "no
        # result set" baseline. Does NOT touch _lastrowid or _executing_task.
        self._reset_execute_state()
        # Set the slot INSIDE the try/finally so a KeyboardInterrupt at the
        # STORE_ATTR/SETUP_FINALLY boundary can't pin it to a completed task.
        try:
            self._executing_task = cur_task

            # Pre-flight reject of empty / multi-statement / wrong-?-count SQL.
            _classify_caller_sql(operation, parameters)

            # Intercept PRAGMA busy_timeout before the wire (dqlite's VFS
            # authorizer rejects it server-side).
            from dqlitedbapi._pragma_intercept import (
                try_intercept_busy_timeout,
                try_rewrite_begin_to_immediate,
            )

            if try_intercept_busy_timeout(self, operation, parameters):
                return self

            # Rewrite plain BEGIN -> BEGIN IMMEDIATE under the default "immediate"
            # session_mode to dodge the SQLITE_BUSY_SNAPSHOT race.
            rewritten = try_rewrite_begin_to_immediate(
                operation,
                session_mode=getattr(self._connection, "_dqlite_session_mode", "immediate"),
            )
            if rewritten is not None:
                operation = rewritten

            _, op_lock = self._connection._ensure_locks()
            # Acquire op_lock per attempt (not around the whole retry loop) so the
            # inter-attempt backoff sleep runs OUTSIDE the lock — sibling
            # commit/rollback/close can acquire op_lock in the gap between BUSY
            # retries instead of parking for the full backoff.
            from dqlitedbapi._busy_retry import (
                _resolve_busy_timeout_seconds,
                retry_async_on_busy,
            )

            async def _attempt() -> None:
                async with op_lock:
                    # Re-clear/re-check per attempt; the under-lock discipline
                    # must hold for every attempt, not just the first.
                    del self.messages[:]
                    self._check_closed()
                    await self._execute_unlocked(operation, parameters)

            await retry_async_on_busy(
                _resolve_busy_timeout_seconds(self._connection),
                _attempt,
            )
        finally:
            # Clear unconditionally to close the bytecode-tight signal window
            # between the guarded read and write of the slot.
            self._executing_task = None

        return self

    async def executemany(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]], /
    ) -> Self:
        """Execute a database operation for each parameter set.

        RETURNING rows are accumulated into _rows so a later fetchall yields all
        rows across parameter sets. Pure queries (SELECT/VALUES/PRAGMA) rejected.

        Cancellation atomicity: autocommit-by-default means each iteration
        commits independently; a mid-batch cancel leaves completed iterations
        persisted. Wrap in explicit BEGIN/COMMIT to make the batch atomic.
        """
        del self.messages[:]
        self._check_closed()
        # Input-validation rejections FIRST (stdlib preserves prior state); the
        # reset below handles prepare-stage rejections which clear.
        if seq_of_parameters is None:
            raise ProgrammingError(
                "executemany() seq_of_parameters must be a sequence/iterable, not None",
                code=None,
            )
        # Reject outer shapes that iterate over keys/chars (dict/str/bytes) or in
        # non-deterministic order (set/frozenset).
        _validate_executemany_seq_shape(seq_of_parameters)
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )
        # Reject concurrent execute/executemany on the same cursor (see execute).
        cur_task = asyncio.current_task()
        if self._executing_task is not None and self._executing_task is not cur_task:
            raise InterfaceError(
                f"cursor is already executing in another task (id={id(self)}); "
                "use one cursor per task"
            )
        # Snapshot BEFORE the reset (for the input-validation-vs-mid-batch contract).
        completed_iterations_pre_batch = self._completed_iterations
        # Scrub per-execute state so a rejected executemany lands at the stdlib
        # baseline. Does NOT touch _lastrowid or _executing_task.
        self._reset_execute_state()
        # Set the slot INSIDE try/finally so any non-success exit clears it.
        try:
            self._executing_task = cur_task
            # Reject transaction-control verbs and pure queries up front. Loop
            # comment-strip + ;-strip together so a leading ``;`` followed by a
            # comment cannot bypass the reject-list.
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
                # Use the comment-stripped uppercase form so a leading SQL comment
                # doesn't route past the PRAGMA-specific diagnostic.
                if head_normalised.startswith("PRAGMA"):
                    raise ProgrammingError(
                        "executemany() does not accept PRAGMA; PRAGMAs have "
                        "per-call semantics and are not batchable. Use "
                        "execute() for each PRAGMA."
                    )
                raise ProgrammingError(
                    "executemany() can only execute DML statements; "
                    "use execute() for SELECT / VALUES / PRAGMA / EXPLAIN / WITH."
                )

            acc = _ExecuteManyAccumulator(max_rows=self._connection._max_total_rows)
            # Hold op_lock once for the whole loop so a concurrent task can't slip
            # COMMIT/ROLLBACK/DDL between iterations of a RETURNING batch.
            _, op_lock = self._connection._ensure_locks()
            async with op_lock:
                del self.messages[:]
                self._check_closed()
                # Snapshot pre-batch lastrowid so a mid-batch cancel restores it
                # rather than leaking an in-batch iteration's rowid (BaseException
                # arm below).
                lastrowid_pre_batch = self._lastrowid
                # _strip_sql_noise neutralises ``?`` in literals/comments so only
                # real placeholders are counted.
                placeholder_count = _strip_sql_noise(operation).count("?")
                try:
                    from dqlitedbapi._busy_retry import (
                        _resolve_busy_timeout_seconds,
                        retry_async_on_busy,
                    )

                    _busy_timeout_seconds = _resolve_busy_timeout_seconds(self._connection)
                    for params in seq_of_parameters:
                        # Re-check per iteration so a concurrent close() between
                        # iterations surfaces as "Cursor is closed".
                        self._check_closed()
                        # Structural reject FIRST (sharp diagnostic for a single
                        # str/bytes/Mapping/set row), THEN the arity check below.
                        # Unsized iterables make len() raise and skip the count
                        # check, falling through to the bind layer.
                        _validate_caller_param_shape(params)
                        if params is not None:
                            try:
                                param_count = len(params)
                            except TypeError:
                                param_count = -1
                            if param_count >= 0 and param_count != placeholder_count:
                                raise ProgrammingError(
                                    f"Incorrect number of bindings supplied. The "
                                    f"current statement uses {placeholder_count}, "
                                    f"and there are {param_count} supplied."
                                )
                        # Per-iteration BUSY retry. Default-arg capture freezes
                        # this iteration's params; a bare closure would late-bind
                        # and every retry would see the LAST iteration's values.
                        _iter_params = params

                        def _execute_iter(
                            _op: str = operation,
                            _p: Sequence[Any] = _iter_params,
                        ) -> Coroutine[Any, Any, None]:
                            return self._execute_unlocked(_op, _p)

                        await retry_async_on_busy(
                            _busy_timeout_seconds,
                            _execute_iter,
                        )
                        self._check_closed()
                        acc.push(self)
                        # push early-returns if a foreign-thread cascade flipped
                        # _closed; only advance the counter while still operable.
                        if not self._closed:
                            self._completed_iterations += 1
                except BaseException:
                    # Mid-batch failure: reset _rowcount to -1 and clear result
                    # state. _lastrowid restores to pre-batch unconditionally —
                    # matching the success path, the docstring, and stdlib, which
                    # leave lastrowid unchanged across executemany.
                    # _completed_iterations is the separate partial-progress anchor
                    # (surfaced via rownumber) and restores only on ZERO in-batch
                    # progress.
                    self._rowcount = -1
                    self._rows = []
                    self._description = None
                    self._row_index = 0
                    self._lastrowid = lastrowid_pre_batch
                    if self._completed_iterations == 0:
                        self._completed_iterations = completed_iterations_pre_batch
                    del self.messages[:]
                    raise
                # Final guard before apply; pairs with the _closed check inside
                # _ExecuteManyAccumulator.apply.
                self._check_closed()
                acc.apply(self)
                # stdlib parity: executemany leaves lastrowid unchanged; restore
                # the pre-batch value rather than exposing an in-batch rowid.
                self._lastrowid = lastrowid_pre_batch
        finally:
            # Clear unconditionally — see ``execute`` finally for the rationale.
            self._executing_task = None
        return self

    def _check_result_set(self) -> None:
        if self._description is None:
            raise ProgrammingError("no results to fetch; execute a query first")

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row, or None when exhausted / no result set active."""
        del self.messages[:]
        self._check_closed()
        # Non-binding loop check so a foreign-loop fetch raises rather than
        # silently succeeding on buffered rows, without lazy-binding a fresh cursor.
        self._connection._check_loop_binding()
        if self._description is None:
            return None  # stdlib parity: no result set -> None, not raise

        return self._next_row_unlocked()

    def _next_row_unlocked(self) -> tuple[Any, ...] | None:
        """Advance one row + apply ``row_factory``, without clearing messages or
        re-running guards (those run once per top-level fetch call)."""
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        # Apply factory BEFORE advancing _row_index so a factory raise leaves the
        # index unchanged (else fetchmany's snapshot/restore replays a row).
        if self._row_factory is not None:
            try:
                transformed: tuple[Any, ...] = self._row_factory(self, row)
            except TypeError as exc:
                # A factory rejecting ``self`` (e.g. sqlite3.Row) leaks bare
                # TypeError past ``except dbapi.Error:``; wrap as DataError.
                raise DataError(
                    f"row_factory call failed: {exc}",
                    code=None,
                    raw_message=str(exc),
                ) from exc
            self._row_index += 1
            return transformed
        self._row_index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to ``size`` rows; empty list when exhausted / no result set.

        size=0 returns [] without consuming rows (stdlib sqlite3 parity; diverges
        from psycopg3 which treats 0 as "use self.arraysize").
        """
        del self.messages[:]
        self._check_closed()
        self._connection._check_loop_binding()
        if self._description is None:
            return []

        if size is None:
            size = self._arraysize
        elif not _is_int_not_bool(size):
            # bool rejected: ``True`` silently coercing to 1 is a caller-bug trap.
            raise ProgrammingError(f"fetchmany expects an int or None, got {type(size).__name__}")
        if size < 0:
            # Stdlib parity: sqlite3 rejects negative size (wrapped to dbapi.Error).
            raise ProgrammingError(f"fetchmany size must be non-negative; got {size}")

        # Snapshot _row_index; on a mid-loop cancel restore to (snapshot +
        # delivered) so consumed-but-undelivered rows aren't lost or replayed.
        # The yield sits AFTER append so len(result) == fully-delivered count.
        snapshot = self._row_index
        result: list[tuple[Any, ...]] = []
        yield_enabled = size >= _LARGE_RESULT_ROW_THRESHOLD
        try:
            for _ in range(size):
                row = self._next_row_unlocked()
                if row is None:
                    break
                result.append(row)
                if yield_enabled and len(result) % _CONVERT_ROWS_YIELD_EVERY == 0:
                    await asyncio.sleep(0)
        except BaseException:
            self._row_index = snapshot + len(result)
            raise

        return result

    async def _apply_row_factory_yielding(
        self, rows: list[tuple[Any, ...]], factory: RowFactory
    ) -> list[tuple[Any, ...]]:
        """Apply ``factory`` to every row, yielding every _CONVERT_ROWS_YIELD_EVERY
        rows on large batches so the per-row pass doesn't monopolise the loop."""
        try:
            if len(rows) < _LARGE_RESULT_ROW_THRESHOLD:
                return [factory(self, row) for row in rows]
            transformed: list[tuple[Any, ...]] = []
            for i, row in enumerate(rows):
                transformed.append(factory(self, row))
                if (i + 1) % _CONVERT_ROWS_YIELD_EVERY == 0:
                    await asyncio.sleep(0)
            return transformed
        except TypeError as exc:
            # Wrap as DataError so a sqlite3.Row-style factory rejection stays in
            # the PEP 249 hierarchy.
            raise DataError(
                f"row_factory call failed: {exc}",
                code=None,
                raw_message=str(exc),
            ) from exc

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows; empty list when exhausted / no result set."""
        del self.messages[:]
        self._check_closed()
        self._connection._check_loop_binding()
        if self._description is None:
            return []

        result = self._rows[self._row_index :]
        if self._row_factory is not None:
            # Advance _row_index only AFTER the transform succeeds so a
            # factory raise / mid-transform cancel leaves the result re-fetchable.
            transformed = await self._apply_row_factory_yielding(result, self._row_factory)
            self._row_index = len(self._rows)
            return transformed
        self._row_index = len(self._rows)
        return result

    def drain_rows(self) -> list[tuple[Any, ...]]:
        """Return the row buffer and clear it (ownership transfer; no row_factory).

        For adapters (SA async) that rebuffer rows and would otherwise pay 2x
        memory. The cursor is unusable for fetch* afterward. Read
        rowcount/lastrowid/description BEFORE draining.
        """
        rows = self._rows
        self._rows = []
        self._row_index = 0
        return rows

    def close(self) -> None:
        """Close the cursor. Idempotent and synchronous by design.

        Synchronous (no await) so a forgotten ``await cur.close()`` raises
        immediately instead of leaving an undrained cursor and a discarded
        coroutine. Clears description / _rows / _row_index but PRESERVES rowcount
        / lastrowid (readable after close, stdlib parity); arraysize is a config
        hint and is also kept. ``await cur.close()`` raises TypeError — drop the
        await, use ``async with cur:``, or call :meth:`aclose`.
        """
        # Suppress AttributeError so close() invoked from __aexit__ after a body
        # exception cannot supplant that exception (PEP 343).
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        if self._closed:
            return
        # Set the flag FIRST so a sibling _execute_unlocked parked on the wire
        # observes it on resume and short-circuits before repopulating state.
        self._closed = True
        self._rows = []
        self._description = None
        # _rowcount / _lastrowid preserved (stdlib parity); _rows/_description
        # cleared because a closed cursor cannot fetch.
        self._row_index = 0
        self._executing_task = None
        # Drop the strong back-reference so a retained closed cursor doesn't pin
        # the connection's loop-bound lock / finalize registration.
        with contextlib.suppress(
            TypeError
        ):  # pragma: no cover - AsyncConnection always supports weakref
            self._connection = weakref.proxy(self._connection)

    async def aclose(self) -> None:
        """Awaitable alias for :meth:`close` (contextlib.aclosing / cross-driver
        ``async def close`` parity); performs a plain sync close."""
        self.close()

    def setinputsizes(self, sizes: Sequence[Any] | None, /) -> None:
        """Set input sizes (no-op for dqlite)."""
        del self.messages[:]
        # Permissive-on-closed: short-circuit BEFORE validators so a closed-cursor
        # cleanup helper can call this regardless of argument shape.
        if self._closed or self._connection._closed:
            return
        # Affinity-before-shape: surface a loop-binding mismatch up front (non-
        # binding so a fresh connection isn't lazily bound).
        self._connection._check_loop_binding()
        if sizes is None:
            return
        if isinstance(sizes, (str, bytes, bytearray, memoryview)):
            # memoryview satisfies Sequence, so reject the quartet explicitly.
            raise ProgrammingError(
                f"setinputsizes expects a sequence of size hints, got {type(sizes).__name__}"
            )
        if not isinstance(sizes, Sequence):
            raise ProgrammingError(f"setinputsizes expects a Sequence, got {type(sizes).__name__}")

    def setoutputsize(self, size: int | None, column: int | None = None, /) -> None:
        """Set output size (no-op for dqlite). See ``setinputsizes``."""
        del self.messages[:]
        if self._closed or self._connection._closed:
            return
        self._connection._check_loop_binding()
        if size is None:
            return
        if not _is_int_not_bool(size):
            raise ProgrammingError(f"setoutputsize expects an int, got {type(size).__name__}")
        if column is not None and not _is_int_not_bool(column):
            raise ProgrammingError(
                f"setoutputsize column expects an int or None, got {type(column).__name__}"
            )

    def callproc(self, procname: str, parameters: Sequence[Any] | None = None, /) -> NoReturn:
        """PEP 249 optional extension — not supported.

        Sync (not async) so a forgotten await raises on the call line, matching
        the sync siblings and the SQLAlchemy adapter.
        """
        del self.messages[:]
        self._check_closed()
        # Loop-binding check so a foreign-loop call doesn't silently surface
        # NotSupportedError while leaving the caller thinking it's still bound.
        self._connection._check_loop_binding()
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> NoReturn:
        """PEP 249 optional extension — not supported."""
        del self.messages[:]
        self._check_closed()
        self._connection._check_loop_binding()
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative", /) -> NoReturn:
        """PEP 249 optional extension — not supported."""
        del self.messages[:]
        self._check_closed()
        self._connection._check_loop_binding()
        # Validate value/mode before raising so a caller typo surfaces as a
        # caller-side bug (bool rejected as an int subclass).
        if not _is_int_not_bool(value):
            raise ProgrammingError(
                f"scroll value must be an integer offset, got {type(value).__name__}"
            )
        if mode not in ("relative", "absolute"):
            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', got {mode!r}")
        raise NotSupportedError("dqlite cursors are not scrollable")

    def executescript(self, sql_script: str, /) -> NoReturn:
        """stdlib ``sqlite3.Cursor``-parity stub — not supported.

        Plain def (not async) so the raise fires on the call line rather than
        being deferred to a forgotten await.
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
        # sanitize_for_log renders control/bidi/ZW chars as ``?``. A GC'd proxy
        # parent raises ReferenceError on attribute access (outside dbapi.Error,
        # and repr is called by debuggers/loggers) so fall back to ``"?"``.
        try:
            address = sanitize_for_log(str(getattr(self._connection, "_address", "?")))
        except ReferenceError:
            address = "?"
        return (
            f"<AsyncCursor address={address!r} rowcount={self._rowcount} {state} at 0x{id(self):x}>"
        )

    def __reduce__(self) -> NoReturn:
        # AsyncCursors hold a loop-bound connection ref that can't survive
        # pickling; surface a clear TypeError instead of the default pickle walk.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — async "
            "cursors hold a reference to a loop-bound driver "
            "connection; use fetchall()/fetchmany() to materialise "
            "rows before crossing a process boundary"
        )

    def _check_parent_loop_only(self) -> None:
        # Used by __aenter__: translate a GC'd-proxy ReferenceError into
        # InterfaceError so ``except dbapi.Error:`` around ``async with cur:``
        # catches the misuse. Live-parent loop-binding fail-fast preserved.
        try:
            self._connection._check_loop_only()
        except ReferenceError as e:
            raise InterfaceError(
                f"Cursor's parent AsyncConnection has been garbage-collected (id={id(self)})"
            ) from e

    def _check_parent_loop_only_lazy(self) -> None:
        """Variant used by __aiter__ that silently defers on a GC'd parent to
        match stdlib's ``iter(closed_cur) is closed_cur``; live-parent
        loop-binding mismatch still raises."""
        try:
            self._connection._check_loop_only()
        except ReferenceError:
            return

    def __aiter__(self) -> Self:
        del self.messages[:]
        # Surface a loop-mismatch at the ``async for cursor:`` site rather than
        # deeper in __anext__. Loop-only (lazy) variant so a closed cursor / GC'd
        # parent still yields ``aiter(cur) is cur`` (closed diagnostic deferred to
        # first __anext__). The iter-time check is a deliberate async-only
        # divergence: async for is the common cross-loop idiom.
        self._check_parent_loop_only_lazy()
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        """Advance the cursor by one row.

        Row-factory raise: the index is NOT advanced, so the next __anext__
        retries the SAME row (the cursor is "wedged" until execute resets the
        buffer). Deliberate — keeps fetchmany's snapshot/restore retry exact.
        """
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        # Cooperative yield: fetchone is sync for buffered rows, so yield every
        # _ANEXT_YIELD_EVERY rows to keep a large ``async for`` from hogging the loop.
        self._aiter_yield_counter += 1
        if self._aiter_yield_counter >= _ANEXT_YIELD_EVERY:
            self._aiter_yield_counter = 0
            await asyncio.sleep(0)
        return row

    async def __aenter__(self) -> Self:
        del self.messages[:]
        # Surface loop-binding mismatches at the ``async with`` line (non-binding
        # so a fresh cursor isn't lazy-bound; GC'd parent -> InterfaceError).
        self._check_parent_loop_only()
        # Cross-task contention guard: __aexit__ closes unconditionally, so without
        # this a foreign task's ``async with cur:`` would close a cursor that
        # Task A is mid-execute on. Surface the misuse here, not later at fetch.
        cur_task = asyncio.current_task()
        if self._executing_task is not None and self._executing_task is not cur_task:
            raise InterfaceError(
                f"cursor is already executing in another task (id={id(self)}); "
                "use one cursor per task"
            )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # ``close`` is now sync (see docstring) — no await needed.
        self.close()
