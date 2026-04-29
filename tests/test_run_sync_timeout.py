"""Tests for _run_sync timeout behavior."""

import asyncio
import logging
from typing import Any

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import OperationalError


class TestRunSyncTimeout:
    def test_run_sync_times_out(self) -> None:
        """_run_sync should raise OperationalError after timeout."""
        conn = Connection("localhost:9001", timeout=0.1)

        async def hang_forever() -> None:
            await asyncio.sleep(999)

        with pytest.raises(OperationalError, match="timed out"):
            conn._run_sync(hang_forever())

    def test_run_sync_logs_unexpected_error_during_cancel_wait(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the bounded cancel-wait window unexpectedly observes
        an exception that is neither CancelledError nor TimeoutError,
        it must surface via DEBUG logging. A bare
        ``suppress(Exception)`` would swallow programmer bugs
        silently; the outer ``OperationalError`` must still be raised,
        but the root cause should be visible to operators.

        Inject a stub Future whose ``result()`` raises ``TimeoutError``
        on the first call (triggering the timeout branch) and
        ``RuntimeError`` on the second call (the bounded cancel-wait).
        The DEBUG log is the only place that RuntimeError becomes
        visible; a naive ``suppress(Exception)`` would eat it.
        """
        import concurrent.futures as cf

        from dqlitedbapi import connection as conn_module

        conn = Connection("localhost:9001", timeout=0.05)

        class _StubFuture:
            def __init__(self) -> None:
                self._calls = 0

            def cancel(self) -> bool:
                return True

            def done(self) -> bool:
                # Race check (ISSUE-674) — pretend the coroutine is
                # still running, so the cancel-success branch is
                # skipped and the original cancel-then-bounded-wait
                # path executes.
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
            # Consume the coroutine so the interpreter doesn't warn.
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
        ), (
            "Bounded cancel-wait should DEBUG-log unexpected errors so "
            "programmer bugs in cleanup paths are observable, not silent."
        )

    def test_run_sync_preserves_coroutine_return_type(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pin the TypeVar narrowing: ``_run_sync(coro)`` must declare
        the same return type as ``coro`` so type-checkers can verify
        downstream uses of the result. A regression widening the
        signature back to ``Any`` silently erases this guarantee and
        would not fail a runtime test, so ``typing.assert_type`` (a
        no-op at runtime, evaluated at type-check time) documents the
        contract. Mirrors the sibling pin on ``_call_client``.
        """
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
        """``future.result(timeout=...)`` raises ``TimeoutError`` when
        the bounded wait expired, but the coroutine may have completed
        successfully on the loop thread between the timeout and our
        cancel attempt. ``_run_sync`` must observe ``future.done()`` /
        ``future.cancelled()`` BEFORE invalidating the connection and
        return the successful result. Otherwise the caller's retry
        logic re-runs a non-idempotent statement, doubling the write.
        """
        import concurrent.futures as cf
        from typing import Any

        from dqlitedbapi import connection as conn_module

        conn = Connection("localhost:9001", timeout=0.05)

        class _LateSuccessFuture:
            """Future whose ``result(timeout=...)`` first raises
            TimeoutError (modelling ``_run_sync``'s bounded wait
            expiring) and then reports the coroutine as
            successfully completed."""

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
                return 1234  # successful completion the cancel raced with

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

        # Must NOT raise OperationalError; must return the late success.
        result = conn._run_sync(_never_runs())
        assert result == 1234


class TestRunSyncTimeoutRecoveredExceptionPreservesClass:
    """Pin: when the bounded ``Future.result(timeout=...)`` expired
    but the coroutine actually completed on the loop thread with a
    server-side exception (e.g. ``IntegrityError``), ``_run_sync``
    surfaces the recovered exception's class directly — NOT a
    ``OperationalError("timed out")`` wrap.

    The wrap-in-OperationalError shape used to break caller-side
    type-based dispatch: ``except IntegrityError:`` would not match,
    and the retry harness would re-run a non-idempotent autocommit
    DML, double-writing. The fix re-raises ``recovered_error``
    directly; the original ``TimeoutError`` is still reachable via
    ``__context__``.
    """

    def test_recovered_integrity_error_propagates_directly(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import concurrent.futures as cf
        from typing import Any

        from dqlitedbapi import connection as conn_module
        from dqlitedbapi.exceptions import IntegrityError

        conn = Connection("localhost:9001", timeout=0.05)

        class _LateIntegrityErrorFuture:
            """Future whose ``result(timeout=...)`` first raises
            TimeoutError, then reports the coroutine as completed-
            with-IntegrityError (e.g. UNIQUE constraint violation
            that landed at the same instant the sync timer fired)."""

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

        # Faithful exception class wins over contract preservation:
        # the IntegrityError must propagate, NOT be wrapped in
        # OperationalError.
        with pytest.raises(IntegrityError, match="UNIQUE constraint failed"):
            conn._run_sync(_never_runs())

    def test_recovered_exception_carries_timeout_in_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The original ``TimeoutError`` is preserved on the raised
        exception's ``__context__`` so callers that need the timeout
        signal for diagnostics can still walk the chain."""
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
            # Python sets __context__ to the in-flight TimeoutError
            # automatically when an except clause raises a new
            # exception.
            assert isinstance(e.__context__, cf.TimeoutError), (
                "the original TimeoutError must be reachable via __context__"
            )
        else:
            pytest.fail("expected IntegrityError to be raised")
