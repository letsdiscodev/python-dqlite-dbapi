"""Background event-loop thread used by the sync surface."""

import asyncio
import concurrent.futures
import contextlib
import threading
from collections.abc import Coroutine
from typing import Any

from dqlitedbapi.exceptions import InterfaceError


class LoopThread:
    """A daemon thread running an asyncio loop; coroutines are submitted and awaited from
    other threads. Started on first use, stopped by :meth:`shutdown`."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._inflight: set[concurrent.futures.Future[Any]] = set()
        self._guard = threading.Lock()

    @property
    def loop(self) -> asyncio.AbstractEventLoop | None:
        return self._loop

    def run[T](self, coro: Coroutine[Any, Any, T]) -> T:
        """Run ``coro`` on the loop thread and block for its result.

        A ``KeyboardInterrupt`` or ``SystemExit`` while blocked cancels the coroutine before
        propagating. A cancellation triggered by :meth:`shutdown` surfaces as
        ``concurrent.futures.CancelledError``.
        """
        try:
            loop = self._ensure_running()
        except BaseException:
            coro.close()
            raise
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        with self._guard:
            self._inflight.add(future)
        try:
            return future.result()
        except BaseException:
            future.cancel()
            raise
        finally:
            with self._guard:
                self._inflight.discard(future)

    def shutdown(self, timeout: float) -> None:
        """Cancel in-flight work, stop the loop and join the thread. Idempotent, never raises."""
        with self._guard:
            loop, thread = self._loop, self._thread
            self._thread = None
        if loop is None:
            return
        if thread is not None and thread.is_alive():
            stopper = _cancel_all_and_stop(loop, timeout)
            try:
                asyncio.run_coroutine_threadsafe(stopper, loop)
            except RuntimeError:
                stopper.close()
            thread.join(timeout + 1.0)
        with self._guard:
            inflight = list(self._inflight)
        for future in inflight:
            with contextlib.suppress(concurrent.futures.InvalidStateError):
                future.set_exception(
                    InterfaceError("Connection closed while an operation was in flight")
                )
        if not loop.is_running():
            with contextlib.suppress(Exception):
                loop.close()

    def _ensure_running(self) -> asyncio.AbstractEventLoop:
        with self._guard:
            if self._thread is not None and self._thread.is_alive() and self._loop is not None:
                return self._loop
            if self._loop is not None and self._loop.is_closed():
                raise InterfaceError("Connection is closed (event loop thread stopped)")
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def main() -> None:
                asyncio.set_event_loop(loop)
                loop.call_soon(started.set)
                loop.run_forever()

            thread = threading.Thread(target=main, name=self._name, daemon=True)
            thread.start()
            started.wait()
            self._loop, self._thread = loop, thread
            return loop


async def _cancel_all_and_stop(loop: asyncio.AbstractEventLoop, timeout: float) -> None:
    me = asyncio.current_task()
    tasks = [t for t in asyncio.all_tasks(loop) if t is not me]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.wait(tasks, timeout=timeout)
    loop.stop()
