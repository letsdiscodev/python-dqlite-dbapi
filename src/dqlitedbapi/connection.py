"""Sync connection: an :class:`AsyncConnection` driven from a background loop thread."""

import concurrent.futures
import contextlib
import os
import sys
import threading
import weakref
from collections.abc import Coroutine, Iterable, Iterator, Sequence
from types import TracebackType
from typing import Any, NoReturn, Self

from dqliteclient import DEFAULT_CLOSE_TIMEOUT_SECONDS, DEFAULT_TIMEOUT_SECONDS, DialFunc
from dqlitedbapi import exceptions as _exc
from dqlitedbapi._loop import LoopThread
from dqlitedbapi._stubs import UnsupportedSqlite3Api
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError
from dqlitedbapi.types import RowFactory
from dqlitewire import DEFAULT_MAX_CONTINUATION_FRAMES, DEFAULT_MAX_TOTAL_ROWS

__all__ = ["Connection"]


class Connection(UnsupportedSqlite3Api):
    """PEP 249 connection.

    Every call runs the matching :class:`AsyncConnection` coroutine on a private loop
    thread. With ``check_same_thread=True`` (default) the connection is confined to its
    creating thread; with ``False`` any thread may use it and calls are serialised, but
    each thread must use its own cursor.
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
        check_same_thread: bool = True,
        session_mode: str | None = None,
    ) -> None:
        if not isinstance(check_same_thread, bool):
            raise ProgrammingError(
                f"check_same_thread must be a bool, got {type(check_same_thread).__name__}"
            )
        self._async = AsyncConnection(
            address,
            database=database,
            timeout=timeout,
            max_total_rows=max_total_rows,
            max_continuation_frames=max_continuation_frames,
            max_message_size=max_message_size,
            trust_server_heartbeat=trust_server_heartbeat,
            close_timeout=close_timeout,
            dial_timeout=dial_timeout,
            attempt_timeout=attempt_timeout,
            dial_func=dial_func,
            busy_timeout=busy_timeout,
            session_mode=session_mode,
        )
        self._close_timeout = close_timeout
        self._check_same_thread = check_same_thread
        self._thread_id = threading.get_ident()
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._loop = LoopThread(f"dqlitedbapi-{address}")
        self._finalizer = weakref.finalize(self, self._loop.shutdown, close_timeout)
        self.messages: list[tuple[type[Exception], Exception]] = []

    # -- plumbing ----------------------------------------------------------------

    def _check_thread(self) -> None:
        if os.getpid() != self._pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct it in the child process "
                f"(created in pid {self._pid}, current pid {os.getpid()})"
            )
        if self._check_same_thread and threading.get_ident() != self._thread_id:
            raise ProgrammingError(
                "dqlitedbapi objects created in a thread can only be used in that same "
                f"thread. The object was created in thread id {self._thread_id} and this "
                f"is thread id {threading.get_ident()}."
            )

    def _run[T](self, coro: Coroutine[Any, Any, T]) -> T:
        try:
            self._check_thread()
        except BaseException:
            coro.close()
            raise
        with self._lock:
            try:
                return self._loop.run(coro)
            except concurrent.futures.CancelledError as exc:
                raise InterfaceError(
                    f"Connection force-closed during operation (id={id(self)})"
                ) from exc
            except (KeyboardInterrupt, SystemExit):
                self.force_close_transport()
                raise

    # -- state -------------------------------------------------------------------

    @property
    def address(self) -> str:
        return self._async.address

    @property
    def closed(self) -> bool:
        return self._async.closed

    @property
    def invalidated(self) -> bool:
        return self._async.invalidated

    @property
    def in_transaction(self) -> bool:
        return self._async.in_transaction

    @property
    def session_mode(self) -> str:
        return self._async.session_mode

    @property
    def default_session_mode(self) -> str:
        return self._async.default_session_mode

    @property
    def busy_timeout(self) -> float:
        return self._async.busy_timeout

    @busy_timeout.setter
    def busy_timeout(self, value: float) -> None:
        self._async.busy_timeout = value

    @property
    def row_factory(self) -> RowFactory | None:
        return self._async.row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        self._async.row_factory = value

    @property
    def text_factory(self) -> type[str]:
        return self._async.text_factory

    @text_factory.setter
    def text_factory(self, value: object) -> None:
        self._async.text_factory = value

    @property
    def autocommit(self) -> bool | int:
        return self._async.autocommit

    @autocommit.setter
    def autocommit(self, value: object) -> None:
        self._async.autocommit = value

    @property
    def isolation_level(self) -> str | None:
        return self._async.isolation_level

    @isolation_level.setter
    def isolation_level(self, value: object) -> None:
        self._async.isolation_level = value

    # -- lifecycle -----------------------------------------------------------------

    def connect(self) -> None:
        """Open the wire session now instead of on first use."""
        self._run(self._async.connect())

    def close(self) -> None:
        """Close the connection and stop its loop thread. Idempotent; does not roll back."""
        self._check_thread()
        if self._async.closed and self._loop.loop is None:
            return
        try:
            if not self._async.closed:
                self._run(self._async.close())
        finally:
            self._loop.shutdown(self._close_timeout)
            self._finalizer.detach()

    def force_close_transport(self) -> None:
        """Drop the socket and stop the loop thread without waiting. Thread-safe, never raises."""
        with contextlib.suppress(Exception):
            self._async.force_close_transport()
        self._loop.shutdown(self._close_timeout)
        self._finalizer.detach()

    # -- transactions -------------------------------------------------------------

    def commit(self) -> None:
        self._run(self._async.commit())

    def rollback(self) -> None:
        self._run(self._async.rollback())

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """``BEGIN`` on entry, ``COMMIT`` on clean exit, ``ROLLBACK`` if the body raises."""
        manager = self._async.transaction()
        self._run(manager.__aenter__())
        try:
            yield
        except BaseException:
            self._run(manager.__aexit__(*sys.exc_info()))
            raise
        self._run(manager.__aexit__(None, None, None))

    def set_session_mode(self, mode: str) -> None:
        self._run(self._async.set_session_mode(mode))

    # -- cursors ------------------------------------------------------------------

    def cursor(self, **unsupported: object) -> Cursor:
        self._check_thread()
        return Cursor(self, self._async.cursor(**unsupported))

    def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> Cursor:
        cursor = self.cursor()
        try:
            if parameters is None:
                cursor.execute(operation)
            else:
                cursor.execute(operation, parameters)
        except BaseException:
            cursor.close()
            raise
        return cursor

    def executemany(self, operation: str, seq_of_parameters: Iterable[Sequence[Any]], /) -> Cursor:
        cursor = self.cursor()
        try:
            cursor.executemany(operation, seq_of_parameters)
        except BaseException:
            cursor.close()
            raise
        return cursor

    # -- protocol -------------------------------------------------------------------

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Commit on clean exit, roll back on exception; does not close (stdlib parity)."""
        if self.closed:
            return
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def __repr__(self) -> str:
        return repr(self._async).replace("AsyncConnection", "Connection", 1)

    def __reduce__(self) -> NoReturn:
        raise TypeError(
            f"cannot pickle {type(self).__name__!r}: it owns a live socket and a thread; "
            "reconstruct the connection from its arguments in the consumer process"
        )


Connection.Cursor = Cursor  # type: ignore[attr-defined]
