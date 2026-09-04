"""Async connection."""

import asyncio
import contextlib
import logging
import math
import os
import weakref
from collections.abc import AsyncIterator, Iterable, Sequence
from types import TracebackType
from typing import Any, Final, NoReturn, Self

from dqliteclient import (
    CLOSE_TIMEOUT_FLOOR,
    CLOSE_TIMEOUT_FLOOR_RATIONALE,
    DEFAULT_CLOSE_TIMEOUT_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    ClusterClient,
    DialFunc,
    DqliteConnection,
    MemoryNodeStore,
    parse_address,
    validate_timeout,
)
from dqlitedbapi import _sql
from dqlitedbapi import exceptions as _exc
from dqlitedbapi._busy import retry_on_busy
from dqlitedbapi._stubs import UnsupportedSqlite3Api
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import (
    AMBIGUOUS_COMMIT_CODES,
    FAILED_TO_CONNECT_PREFIX,
    AmbiguousCommitError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    call,
    is_no_transaction_error,
    translate,
)
from dqlitedbapi.types import RowFactory, is_int_not_bool
from dqlitewire import DEFAULT_MAX_CONTINUATION_FRAMES, DEFAULT_MAX_TOTAL_ROWS

__all__ = ["AsyncConnection"]

logger = logging.getLogger(__name__)

# stdlib ``isolation_level`` values (all no-ops here: dqlite is autocommit-by-default).
_ISOLATION_LEVELS: Final[frozenset[str]] = frozenset({"", "DEFERRED", "IMMEDIATE", "EXCLUSIVE"})


class AsyncConnection(UnsupportedSqlite3Api):
    """Connection to a dqlite cluster, bound to the event loop it first runs on.

    Operations are serialised by an internal lock, so concurrent tasks queue rather
    than fail. Statements autocommit unless an explicit ``BEGIN`` is open.
    """

    Error = _exc.Error
    Warning = _exc.Warning  # noqa: A003 - PEP 249 mandated name
    InterfaceError = _exc.InterfaceError
    DatabaseError = _exc.DatabaseError
    DataError = _exc.DataError
    OperationalError = _exc.OperationalError
    IntegrityError = _exc.IntegrityError
    InternalError = _exc.InternalError
    ProgrammingError = _exc.ProgrammingError
    NotSupportedError = _exc.NotSupportedError
    AmbiguousCommitError = _exc.AmbiguousCommitError

    def __init__(
        self,
        address: str,
        *,
        database: str = "default",
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_total_rows: int | None = DEFAULT_MAX_TOTAL_ROWS,
        max_continuation_frames: int | None = DEFAULT_MAX_CONTINUATION_FRAMES,
        max_message_size: int | None = None,
        trust_server_heartbeat: bool = False,
        close_timeout: float = DEFAULT_CLOSE_TIMEOUT_SECONDS,
        dial_timeout: float | None = None,
        attempt_timeout: float | None = None,
        dial_func: DialFunc | None = None,
        busy_timeout: float = 5.0,
        session_mode: str | None = None,
    ) -> None:
        if not isinstance(address, str):
            raise ProgrammingError(
                f"address must be a 'host:port' string, got {type(address).__name__}"
            )
        try:
            parse_address(address)
        except ValueError as e:
            raise ProgrammingError(f"Invalid address: {e}") from e
        if not isinstance(database, str) or not database or database != database.strip():
            raise ProgrammingError(
                "database must be a non-empty string without surrounding whitespace"
            )
        try:
            validate_timeout(
                close_timeout,
                name="close_timeout",
                min_value=CLOSE_TIMEOUT_FLOOR,
                min_value_rationale=CLOSE_TIMEOUT_FLOOR_RATIONALE,
            )
            _validate_busy_timeout(busy_timeout)
            # Validates the remaining knobs at construction time.
            self._cluster = ClusterClient(
                MemoryNodeStore([address]),
                timeout=timeout,
                dial_timeout=dial_timeout,
                attempt_timeout=attempt_timeout,
                max_total_rows=max_total_rows,
                max_continuation_frames=max_continuation_frames,
                max_message_size=max_message_size,
                trust_server_heartbeat=trust_server_heartbeat,
                dial_func=dial_func,
            )
            resolved_mode = (
                _sql.default_session_mode()
                if session_mode is None
                else _sql.validate_session_mode(session_mode)
            )
        except (TypeError, ValueError) as exc:
            raise ProgrammingError(str(exc)) from exc
        self._address = address
        self._database = database
        self._timeout = timeout
        self._close_timeout = close_timeout
        self._max_total_rows = max_total_rows
        self._connect_options: dict[str, Any] = {
            "max_total_rows": max_total_rows,
            "max_continuation_frames": max_continuation_frames,
            "max_message_size": max_message_size,
            "trust_server_heartbeat": trust_server_heartbeat,
            "close_timeout": close_timeout,
        }
        self._busy_timeout = float(busy_timeout)
        self._session_mode = resolved_mode
        self._default_session_mode = resolved_mode
        self._client: DqliteConnection | None = None
        self._closed = False
        self._invalidated = False
        self._lock: asyncio.Lock | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._in_ctx_transaction = False
        self._row_factory: RowFactory | None = None
        self._autocommit: bool | int = True
        self._isolation_level: str | None = None
        self._pid = os.getpid()
        self._cursors: weakref.WeakSet[AsyncCursor] = weakref.WeakSet()
        self.messages: list[tuple[type[Exception], Exception]] = []

    # -- state -----------------------------------------------------------------

    @property
    def address(self) -> str:
        return self._address

    @property
    def closed(self) -> bool:
        """True after :meth:`close` or once the wire session was lost."""
        return self._closed or self._invalidated

    @property
    def invalidated(self) -> bool:
        """True when the wire session was lost without an explicit :meth:`close`."""
        return self._invalidated and not self._closed

    @property
    def in_transaction(self) -> bool:
        client = self._client
        return client is not None and not self.closed and client.in_transaction

    @property
    def session_mode(self) -> str:
        return self._session_mode

    @property
    def default_session_mode(self) -> str:
        """The mode given at construction; what a pool should reset to."""
        return self._default_session_mode

    @property
    def busy_timeout(self) -> float:
        """Seconds to keep retrying ``SQLITE_BUSY``; 0 disables retries."""
        return self._busy_timeout

    @busy_timeout.setter
    def busy_timeout(self, value: float) -> None:
        try:
            _validate_busy_timeout(value)
        except (TypeError, ValueError) as exc:
            raise ProgrammingError(str(exc)) from exc
        self._busy_timeout = float(value)

    @property
    def max_total_rows(self) -> int | None:
        return self._max_total_rows

    @property
    def row_factory(self) -> RowFactory | None:
        """Default ``row_factory`` for new cursors."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        if value is not None and not callable(value):
            raise ProgrammingError(
                f"row_factory must be callable or None, got {type(value).__name__}"
            )
        self._row_factory = value

    @property
    def text_factory(self) -> type[str]:
        return str

    @text_factory.setter
    def text_factory(self, value: object) -> None:
        if value is not str:
            raise NotSupportedError("text_factory is not supported; TEXT is always returned as str")

    @property
    def autocommit(self) -> bool | int:
        """Always on. The setter accepts ``True`` and stdlib's ``LEGACY_TRANSACTION_CONTROL``."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: object) -> None:
        if value is True or (is_int_not_bool(value) and value == -1):
            self._autocommit = value
            return
        raise NotSupportedError(
            "dqlite is autocommit-by-default; autocommit cannot be turned off. "
            "Use explicit BEGIN / COMMIT to group statements."
        )

    @property
    def isolation_level(self) -> str | None:
        """stdlib parity only; the accepted values are no-ops."""
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, value: object) -> None:
        if value is None:
            self._isolation_level = None
        elif isinstance(value, str) and value.upper() in _ISOLATION_LEVELS:
            self._isolation_level = value.upper()
        else:
            raise ProgrammingError(
                f"isolation_level must be None or one of {sorted(_ISOLATION_LEVELS)!r}; "
                f"got {value!r}. dqlite has a single isolation level (SERIALIZABLE)."
            )

    # -- guards ------------------------------------------------------------------

    def _check_usable(self) -> None:
        if os.getpid() != self._pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct it in the child process "
                f"(created in pid {self._pid}, current pid {os.getpid()})"
            )
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if self._invalidated:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}): the wire session was lost; "
                "open a new connection"
            )

    def _get_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if self._lock is None:
            self._lock = asyncio.Lock()
            self._loop = loop
        elif self._loop is not loop:
            raise InterfaceError(
                "AsyncConnection is bound to a different event loop; connections cannot be "
                "shared across loops or asyncio.run() calls"
            )
        return self._lock

    @contextlib.asynccontextmanager
    async def _operation(self) -> AsyncIterator[DqliteConnection]:
        """Hold the operation lock and yield the connected client; marks the connection
        invalidated if the client dropped the wire during the operation."""
        self._check_usable()
        async with self._get_lock():
            self._check_usable()
            client = await self._ensure_client()
            try:
                yield client
            finally:
                if not client.is_connected and not self._closed:
                    self._invalidated = True
                    logger.debug("AsyncConnection %s invalidated (wire session lost)", id(self))

    async def _ensure_client(self) -> DqliteConnection:
        if self._client is not None:
            return self._client
        try:
            client = await self._cluster.connect(self._database, **self._connect_options)
        except Exception as exc:
            mapped = translate(exc)
            if mapped is None:
                raise
            if not str(mapped).startswith(_exc.CLUSTER_POLICY_REJECTION_PREFIX):
                mapped = type(mapped)(
                    f"{FAILED_TO_CONNECT_PREFIX}{mapped}",
                    code=mapped.code,
                    raw_message=mapped.raw_message,
                )
            raise mapped from exc
        if self._session_mode == "read_only":
            try:
                await call(client.execute("PRAGMA query_only = 1"))
            except BaseException:
                client.terminate()
                raise
        self._client = client
        return client

    # -- lifecycle -------------------------------------------------------------

    async def connect(self) -> None:
        """Open the wire session now instead of on first use."""
        async with self._operation():
            pass

    async def close(self) -> None:
        """Close the connection. Idempotent; does not roll back."""
        if self._closed:
            return
        self._closed = True
        self._close_cursors()
        client, self._client = self._client, None
        if client is None or os.getpid() != self._pid:
            return
        try:
            async with asyncio.timeout(self._timeout):
                if self._lock is None:
                    await client.close()
                else:
                    async with self._lock:
                        await client.close()
        except TimeoutError:
            client.terminate()
        except asyncio.CancelledError:
            client.terminate()
            raise

    async def aclose(self) -> None:
        await self.close()

    def force_close_transport(self) -> None:
        """Drop the socket without waiting for in-flight work. Thread-safe, never raises."""
        self._closed = True
        self._close_cursors()
        client, self._client = self._client, None
        if client is None or os.getpid() != self._pid:
            return
        loop = self._loop
        running: asyncio.AbstractEventLoop | None = None
        with contextlib.suppress(RuntimeError):
            running = asyncio.get_running_loop()
        if loop is None or loop.is_closed() or running is loop:
            client.terminate()
        else:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(client.terminate)

    def _close_cursors(self) -> None:
        for cursor in list(self._cursors):
            cursor.close()
        self._cursors.clear()

    # -- transactions ----------------------------------------------------------

    async def commit(self) -> None:
        """Commit the open transaction; a no-op when none is open."""
        await self._end_transaction("COMMIT")

    async def rollback(self) -> None:
        """Roll back the open transaction; a no-op when none is open."""
        await self._end_transaction("ROLLBACK")

    async def _end_transaction(self, verb: str) -> None:
        self._check_usable()
        if self._in_ctx_transaction:
            raise InterfaceError(
                f"{verb.lower()}() cannot be called inside transaction(); "
                "the context manager owns the transaction boundaries"
            )
        if self._client is None:
            return
        async with self._operation() as client:
            if not client.in_transaction:
                return
            try:
                await retry_on_busy(self._busy_timeout, lambda: call(client.execute(verb)))
            except OperationalError as exc:
                if is_no_transaction_error(exc):
                    return
                if verb == "COMMIT" and exc.code in AMBIGUOUS_COMMIT_CODES:
                    raise AmbiguousCommitError(
                        "ambiguous commit: leadership lost during COMMIT; the write may or "
                        f"may not have been persisted. Original: {exc}",
                        code=exc.code,
                        raw_message=exc.raw_message,
                    ) from exc
                raise

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """``BEGIN`` on entry, ``COMMIT`` on clean exit, ``ROLLBACK`` if the body raises."""
        self._check_usable()
        if self._in_ctx_transaction:
            raise InterfaceError("Nested transaction() blocks are not supported; use SAVEPOINT")
        await self.cursor().execute("BEGIN")
        self._in_ctx_transaction = True
        try:
            yield
        except BaseException:
            self._in_ctx_transaction = False
            if not self.closed:
                try:
                    await self.rollback()
                except Exception:
                    logger.debug("rollback after failed transaction() body raised", exc_info=True)
                    self.force_close_transport()
            raise
        self._in_ctx_transaction = False
        await self.commit()

    async def set_session_mode(self, mode: str) -> None:
        """Switch session mode, emitting ``PRAGMA query_only`` across the read_only boundary."""
        try:
            mode = _sql.validate_session_mode(mode)
        except ValueError as exc:
            raise ProgrammingError(str(exc)) from exc
        if mode == self._session_mode:
            return
        async with self._operation() as client:
            if client.in_transaction:
                raise InterfaceError("set_session_mode() cannot be called inside a transaction")
            read_only_now, read_only_next = self._session_mode == "read_only", mode == "read_only"
            if read_only_now != read_only_next:
                await call(client.execute(f"PRAGMA query_only = {int(read_only_next)}"))
            self._session_mode = mode

    # -- cursors -----------------------------------------------------------------

    def cursor(self, **unsupported: object) -> AsyncCursor:
        if unsupported:
            raise NotSupportedError(
                f"cursor() does not support keyword(s) {sorted(unsupported)}; "
                "cursor subclassing via factory= is not available"
            )
        self._check_usable()
        cursor = AsyncCursor(self)
        self._cursors.add(cursor)
        return cursor

    async def execute(
        self, operation: str, parameters: Sequence[Any] | None = None, /
    ) -> AsyncCursor:
        """Open a cursor, execute, and return it."""
        cursor = self.cursor()
        try:
            if parameters is None:
                await cursor.execute(operation)
            else:
                await cursor.execute(operation, parameters)
        except BaseException:
            cursor.close()
            raise
        return cursor

    async def executemany(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]], /
    ) -> AsyncCursor:
        cursor = self.cursor()
        try:
            await cursor.executemany(operation, seq_of_parameters)
        except BaseException:
            cursor.close()
            raise
        return cursor

    # -- protocol ----------------------------------------------------------------

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Commit on clean exit, roll back on exception; does not close (stdlib parity)."""
        if self.closed or self._client is None:
            return
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()

    def __repr__(self) -> str:
        if self._closed:
            state = "closed"
        elif self._invalidated:
            state = "invalidated"
        else:
            state = "connected" if self._client is not None else "unused"
        return f"<AsyncConnection address={self._address!r} database={self._database!r} {state}>"

    def __reduce__(self) -> NoReturn:
        raise TypeError(
            f"cannot pickle {type(self).__name__!r}: it owns a live socket; "
            "reconstruct the connection from its arguments in the consumer process"
        )


# aiosqlite-style class attributes for cross-driver isinstance checks.
AsyncConnection.Cursor = AsyncCursor  # type: ignore[attr-defined]
AsyncConnection.AsyncCursor = AsyncCursor  # type: ignore[attr-defined]


def _validate_busy_timeout(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"busy_timeout must be a number (seconds), got {type(value).__name__}")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"busy_timeout must be a non-negative finite number, got {value}")


def apply_stdlib_connect_kwargs(connection: AsyncConnection, kwargs: dict[str, object]) -> None:
    """Honour stdlib ``sqlite3.connect`` keywords the driver can model; reject the rest."""
    if "isolation_level" in kwargs:
        connection.isolation_level = kwargs.pop("isolation_level")
    if "autocommit" in kwargs:
        connection.autocommit = kwargs.pop("autocommit")
    if kwargs:
        raise NotSupportedError(
            f"connect() does not support stdlib sqlite3 keyword(s) {sorted(kwargs)}"
        )
