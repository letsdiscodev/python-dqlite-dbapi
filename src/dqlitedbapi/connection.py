"""PEP 249 Connection implementation for dqlite."""

import asyncio
import contextlib
import threading
import warnings
import weakref
from typing import Any

from dqliteclient import DqliteConnection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError, OperationalError, ProgrammingError


def _cleanup_loop_thread(
    loop: asyncio.AbstractEventLoop,
    thread: threading.Thread,
    closed_flag: list[bool],
    address: str,
) -> None:
    """Stop the background event loop and join its thread.

    Called from a ``weakref.finalize`` so it must not reference the
    ``Connection`` instance. ``closed_flag`` is a 1-element list that
    the Connection mutates when ``close()`` is called — we use that
    rather than a direct reference to self to decide whether to emit
    a ``ResourceWarning``.
    """
    if closed_flag[0] is False:
        # User never called close() → leak warning (matches stdlib
        # sqlite3). Don't crash at interpreter shutdown.
        try:
            warnings.warn(
                f"Connection(address={address!r}) was garbage-collected "
                f"without close(); cleaning up event-loop thread. Call "
                f"Connection.close() explicitly to avoid this warning.",
                ResourceWarning,
                stacklevel=2,
            )
        except Exception:
            pass
    try:
        if not loop.is_closed():
            loop.call_soon_threadsafe(loop.stop)
    except Exception:
        pass
    try:
        thread.join(timeout=5)
    except Exception:
        pass
    try:
        if not loop.is_closed():
            loop.close()
    except Exception:
        pass


class Connection:
    """PEP 249 compliant database connection."""

    def __init__(
        self,
        address: str,
        *,
        database: str = "default",
        timeout: float = 10.0,
    ) -> None:
        """Initialize connection (does not connect yet).

        Args:
            address: Node address in "host:port" format
            database: Database name to open
            timeout: Connection timeout in seconds
        """
        self._address = address
        self._database = database
        self._timeout = timeout
        self._async_conn: DqliteConnection | None = None
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._loop_lock = threading.Lock()
        self._op_lock = threading.Lock()
        self._connect_lock: asyncio.Lock | None = None
        self._creator_thread = threading.get_ident()
        # 1-element list (mutable, captured by the finalizer) that
        # close() flips to True. Using a list avoids the finalizer
        # closing over ``self`` and preventing GC.
        self._closed_flag: list[bool] = [False]
        self._finalizer: weakref.finalize | None = None

    def _check_thread(self) -> None:
        """Raise ProgrammingError if called from a different thread than the creator."""
        current = threading.get_ident()
        if current != self._creator_thread:
            raise ProgrammingError(
                f"Connection objects created in a thread can only be used in that "
                f"same thread. The object was created in thread id "
                f"{self._creator_thread} and this is thread id {current}."
            )

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Ensure a dedicated event loop is running in a background thread.

        This allows sync methods to work even when called from within
        an already-running async context (e.g. uvicorn).

        Registers a ``weakref.finalize`` the first time the loop is
        created so a Connection that's garbage-collected without an
        explicit ``close()`` still cleans up its thread. (GC'd connections
        used to leak daemon threads forever.)
        """
        if self._loop is not None and not self._loop.is_closed():
            return self._loop
        with self._loop_lock:
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
                self._thread.start()
                # Finalizer can't close over self — it'd keep the
                # Connection alive. Capture primitives only. The
                # closed-flag list is mutated by close() so the
                # finalizer knows whether to emit a leak warning.
                self._finalizer = weakref.finalize(
                    self,
                    _cleanup_loop_thread,
                    self._loop,
                    self._thread,
                    self._closed_flag,
                    self._address,
                )
        return self._loop

    def _run_sync(self, coro: Any) -> Any:
        """Run an async coroutine from sync code.

        Submits the coroutine to the dedicated background event loop
        and blocks until the result is available. The operation lock
        ensures only one operation runs at a time, preventing wire
        protocol corruption from concurrent access.

        On sync-side timeout we cancel the future AND invalidate the
        underlying connection. The coroutine may have already written
        partial bytes to the socket before observing the cancel;
        invalidation poisons the wire stream so the next operation
        reconnects instead of reusing a torn protocol state.
        """
        with self._op_lock:
            loop = self._ensure_loop()
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            try:
                # Future.result() provides a happens-before memory barrier,
                # ensuring all writes by the event loop thread are visible here.
                return future.result(timeout=self._timeout)
            except TimeoutError as e:
                future.cancel()
                # Poison the underlying connection. The coroutine may have
                # half-written a request; the wire is in unknown state.
                # Fire-and-forget on the loop thread (don't await).
                if self._async_conn is not None:
                    try:
                        loop.call_soon_threadsafe(
                            self._async_conn._invalidate,
                            OperationalError(f"sync timeout after {self._timeout}s"),
                        )
                    except RuntimeError:
                        pass  # loop already shutting down
                raise OperationalError(f"Operation timed out after {self._timeout} seconds") from e

    async def _get_async_connection(self) -> DqliteConnection:
        """Get or create the underlying async connection."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            return self._async_conn

        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()

        async with self._connect_lock:
            if self._async_conn is not None:
                return self._async_conn

            conn = DqliteConnection(
                self._address,
                database=self._database,
                timeout=self._timeout,
            )
            try:
                await conn.connect()
            except Exception as e:
                raise OperationalError(f"Failed to connect: {e}") from e
            self._async_conn = conn

        return self._async_conn

    def close(self) -> None:
        """Close the connection."""
        self._check_thread()
        if self._closed:
            return
        self._closed = True
        # Flip the flag the finalizer reads so it knows this was an
        # explicit close (no ResourceWarning).
        self._closed_flag[0] = True
        # Detach the finalizer — it's about to do nothing useful, and
        # keeping it registered would double-stop the loop.
        if self._finalizer is not None:
            self._finalizer.detach()
            self._finalizer = None
        try:
            if self._loop is not None and not self._loop.is_closed():
                with contextlib.suppress(Exception):
                    self._run_sync(self._close_async())
        finally:
            with self._loop_lock:
                if self._loop is not None and not self._loop.is_closed():
                    self._loop.call_soon_threadsafe(self._loop.stop)
                    if self._thread is not None:
                        self._thread.join(timeout=5)
                    self._loop.close()
                    self._loop = None
                    self._thread = None

    async def _close_async(self) -> None:
        """Async implementation of close -- runs on event loop thread."""
        if self._async_conn is not None:
            try:
                await self._async_conn.close()
            finally:
                self._async_conn = None

    def commit(self) -> None:
        """Commit any pending transaction.

        If the connection has never been used, this is a silent no-op
        (matches stdlib ``sqlite3`` and the existing "no spurious
        connect" contract). If the server reports "no transaction is
        active," that too is swallowed — stdlib ``sqlite3.commit()``
        silently succeeds in the same case, and callers should not
        have to tell the difference between an empty transaction and
        a successfully committed one.
        """
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._async_conn is None:
            return
        self._run_sync(self._commit_async())

    async def _commit_async(self) -> None:
        """Async implementation of commit."""
        assert self._async_conn is not None
        import dqliteclient.exceptions as _client_exc

        try:
            await self._async_conn.execute("COMMIT")
        except (OperationalError, _client_exc.OperationalError) as e:
            if "no transaction is active" not in str(e).lower():
                raise

    def rollback(self) -> None:
        """Roll back any pending transaction.

        Same silent-success contract as :meth:`commit` for "no active
        transaction" and for never-used connections.
        """
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._async_conn is None:
            return
        self._run_sync(self._rollback_async())

    async def _rollback_async(self) -> None:
        """Async implementation of rollback."""
        assert self._async_conn is not None
        import dqliteclient.exceptions as _client_exc

        try:
            await self._async_conn.execute("ROLLBACK")
        except (OperationalError, _client_exc.OperationalError) as e:
            if "no transaction is active" not in str(e).lower():
                raise

    def cursor(self) -> Cursor:
        """Return a new Cursor object."""
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self)

    def __repr__(self) -> str:
        state = "closed" if self._closed else ("connected" if self._async_conn else "unused")
        return (
            f"<Connection address={self._address!r} database={self._database!r} {state}>"
        )

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type: type[BaseException] | None, *args: Any) -> None:
        # If no query has ever run, there's no transaction to commit or
        # roll back — just close.
        if self._async_conn is None:
            self.close()
            return
        try:
            if exc_type is None:
                # Clean exit: commit. Let exceptions propagate; silent
                # data loss is worse than a noisy failure.
                self.commit()
            else:
                # Body already raised; attempt rollback but don't mask
                # the original exception. If rollback itself fails, its
                # error is attached via __context__ automatically.
                try:
                    self.rollback()
                except Exception:
                    pass  # Body's exception wins; rollback failure is attached as context.
        finally:
            self.close()
