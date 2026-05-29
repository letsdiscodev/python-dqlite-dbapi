"""Tests for _run_sync timeout behavior."""

import asyncio
import logging
from typing import Any

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import OperationalError


class TestRunSyncTimeout:
    def test_run_sync_times_out(self) -> None:
        conn = Connection("localhost:9001", timeout=0.1)

        async def hang_forever() -> None:
            await asyncio.sleep(999)

        with pytest.raises(OperationalError, match="timed out"):
            conn._run_sync(hang_forever())

    def test_run_sync_logs_unexpected_error_during_cancel_wait(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unexpected (non-Cancelled/Timeout) error in the cancel-wait must DEBUG-log,
        not be silently swallowed; the outer OperationalError still raises."""
        import concurrent.futures as cf

        from dqlitedbapi import connection as conn_module

        conn = Connection("localhost:9001", timeout=0.05)

        class _StubFuture:
            def __init__(self) -> None:
                self._calls = 0

            def cancel(self) -> bool:
                return True

            def done(self) -> bool:
                # Pretend still running so the cancel-then-bounded-wait path executes.
                return False

            def cancelled(self) -> bool:
                return False

            def result(self, timeout: float | None = None) -> None:
                self._calls += 1
                if self._calls == 1:
                    raise cf.TimeoutError()
                raise RuntimeError("surprise bug during cancel")

        stub = _StubFuture()

        def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _StubFuture:
            coro.close()
            return stub

        monkeypatch.setattr(
            conn_module.asyncio,  # type: ignore[attr-defined]
            "run_coroutine_threadsafe",
            _fake_run_coroutine_threadsafe,
        )

        async def _never_runs() -> None:
            await asyncio.sleep(999)

        caplog.set_level(logging.DEBUG, logger="dqlitedbapi.connection")
        with pytest.raises(OperationalError, match="timed out"):
            conn._run_sync(_never_runs())

        assert any(
            "unexpected error" in rec.message.lower() and rec.levelno == logging.DEBUG
            for rec in caplog.records
        ), "Bounded cancel-wait should DEBUG-log unexpected errors, not swallow them silently."

    def test_run_sync_preserves_coroutine_return_type(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pin the TypeVar narrowing: ``_run_sync(coro)`` returns coro's type, checked via
        ``assert_type`` (a regression to ``Any`` would not fail a runtime-only test)."""
        from typing import assert_type

        from dqlitedbapi import connection as conn_module

        conn = Connection("localhost:9001", timeout=5.0)

        class _ReadyFuture:
            def cancel(self) -> bool:
                return True

            def result(self, timeout: float | None = None) -> int:
                return 7

        def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _ReadyFuture:
            coro.close()
            return _ReadyFuture()

        monkeypatch.setattr(
            conn_module.asyncio,  # type: ignore[attr-defined]
            "run_coroutine_threadsafe",
            _fake_run_coroutine_threadsafe,
        )

        async def returns_int() -> int:
            return 7

        result = conn._run_sync(returns_int())
        assert_type(result, int)
        assert result == 7


class TestRunSyncCancelSuccessRace:
    def test_run_sync_returns_value_when_cancel_loses_race(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the coroutine completes between the bounded-wait TimeoutError and our cancel,
        ``_run_sync`` must return the late success, not invalidate (else a retry double-writes)."""
        import concurrent.futures as cf
        from typing import Any

        from dqlitedbapi import connection as conn_module

        conn = Connection("localhost:9001", timeout=0.05)

        class _LateSuccessFuture:
            """result() raises TimeoutError first, then returns the late success."""

            def __init__(self) -> None:
                self._calls = 0

            def cancel(self) -> bool:
                return False  # cancel lost the race

            def done(self) -> bool:
                return True

            def cancelled(self) -> bool:
                return False

            def result(self, timeout: float | None = None) -> int:
                self._calls += 1
                if self._calls == 1:
                    raise cf.TimeoutError()
                return 1234

        stub = _LateSuccessFuture()

        def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _LateSuccessFuture:
            coro.close()
            return stub

        monkeypatch.setattr(
            conn_module.asyncio,  # type: ignore[attr-defined]
            "run_coroutine_threadsafe",
            _fake_run_coroutine_threadsafe,
        )

        async def _never_runs() -> None:
            await asyncio.sleep(999)

        result = conn._run_sync(_never_runs())
        assert result == 1234


class TestRunSyncTimeoutRecoveredExceptionPreservesClass:
    """Pin: when the bounded wait expired but the coroutine completed with a server-side
    exception (e.g. IntegrityError), ``_run_sync`` re-raises that class directly, not an
    OperationalError wrap (which would break ``except IntegrityError`` dispatch)."""

    def test_recovered_integrity_error_propagates_directly(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import concurrent.futures as cf
        from typing import Any

        from dqlitedbapi import connection as conn_module
        from dqlitedbapi.exceptions import IntegrityError

        conn = Connection("localhost:9001", timeout=0.05)

        class _LateIntegrityErrorFuture:
            """result() raises TimeoutError first, then the recovered IntegrityError."""

            def __init__(self) -> None:
                self._calls = 0

            def cancel(self) -> bool:
                return False

            def done(self) -> bool:
                return True

            def cancelled(self) -> bool:
                return False

            def result(self, timeout: float | None = None) -> Any:
                self._calls += 1
                if self._calls == 1:
                    raise cf.TimeoutError()
                raise IntegrityError(
                    "UNIQUE constraint failed", code=2067, raw_message="UNIQUE constraint failed"
                )

        stub = _LateIntegrityErrorFuture()

        def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _LateIntegrityErrorFuture:
            coro.close()
            return stub

        monkeypatch.setattr(
            conn_module.asyncio,  # type: ignore[attr-defined]
            "run_coroutine_threadsafe",
            _fake_run_coroutine_threadsafe,
        )

        async def _never_runs() -> None:
            await asyncio.sleep(999)

        with pytest.raises(IntegrityError, match="UNIQUE constraint failed"):
            conn._run_sync(_never_runs())

    def test_recovered_exception_carries_timeout_in_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The original TimeoutError stays reachable via the raised exception's ``__context__``."""
        import concurrent.futures as cf
        from typing import Any

        from dqlitedbapi import connection as conn_module
        from dqlitedbapi.exceptions import IntegrityError

        conn = Connection("localhost:9001", timeout=0.05)

        class _Stub:
            def __init__(self) -> None:
                self._calls = 0

            def cancel(self) -> bool:
                return False

            def done(self) -> bool:
                return True

            def cancelled(self) -> bool:
                return False

            def result(self, timeout: float | None = None) -> Any:
                self._calls += 1
                if self._calls == 1:
                    raise cf.TimeoutError()
                raise IntegrityError("constraint failed", code=2067, raw_message="x")

        stub = _Stub()
        monkeypatch.setattr(
            conn_module.asyncio,  # type: ignore[attr-defined]
            "run_coroutine_threadsafe",
            lambda coro, loop: (coro.close(), stub)[1],
        )

        async def _never_runs() -> None:
            await asyncio.sleep(999)

        try:
            conn._run_sync(_never_runs())
        except IntegrityError as e:
            assert isinstance(e.__context__, cf.TimeoutError), (
                "the original TimeoutError must be reachable via __context__"
            )
        else:
            pytest.fail("expected IntegrityError to be raised")


class TestRunSyncTimeoutSynchronousNullOutOfAsyncConn:
    """The TimeoutError arm must synchronously null ``self._async_conn`` (like the KI/SystemExit
    arm) so the next call reconnects even before the loop drains the scheduled ``_invalidate``."""

    def test_timeout_arm_nulls_async_conn_synchronously(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import concurrent.futures as cf
        from typing import Any

        from dqlitedbapi import connection as conn_module

        conn = Connection("localhost:9001", timeout=0.05)

        # Stand in for AsyncConnection so the cleanup arm's not-None guard passes.
        class _StubAsyncConn:
            def _invalidate(self, exc: BaseException | None = None) -> None:
                pass

        conn._async_conn = _StubAsyncConn()  # type: ignore[assignment]

        class _StubFuture:
            def __init__(self) -> None:
                self._calls = 0

            def cancel(self) -> bool:
                return True

            def done(self) -> bool:
                return False

            def cancelled(self) -> bool:
                return False

            def result(self, timeout: float | None = None) -> None:
                self._calls += 1
                if self._calls == 1:
                    raise cf.TimeoutError()
                raise cf.CancelledError()  # bounded-wait: coroutine unwound cleanly

        stub = _StubFuture()

        def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _StubFuture:
            coro.close()
            return stub

        monkeypatch.setattr(
            conn_module.asyncio,  # type: ignore[attr-defined]
            "run_coroutine_threadsafe",
            _fake_run_coroutine_threadsafe,
        )

        async def _never_runs() -> None:
            await asyncio.sleep(999)

        with pytest.raises(OperationalError, match="timed out"):
            conn._run_sync(_never_runs())

        assert conn._async_conn is None, (
            "TimeoutError arm must null self._async_conn synchronously, "
            "mirroring the (KeyboardInterrupt, SystemExit) arm — otherwise "
            "the next sync op wedges on a stale _in_use=True until the "
            "slow loop-side read deadline fires."
        )
