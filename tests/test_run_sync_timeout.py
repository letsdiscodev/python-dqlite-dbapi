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

        def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _StubFuture:  # type: ignore[no-untyped-def]
            # Consume the coroutine so the interpreter doesn't warn.
            coro.close()
            return stub

        monkeypatch.setattr(
            conn_module.asyncio,
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

        def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _ReadyFuture:  # type: ignore[no-untyped-def]
            coro.close()
            return _ReadyFuture()

        monkeypatch.setattr(
            conn_module.asyncio,
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

        def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _LateSuccessFuture:  # type: ignore[no-untyped-def]
            coro.close()
            return stub

        monkeypatch.setattr(
            conn_module.asyncio,
            "run_coroutine_threadsafe",
            _fake_run_coroutine_threadsafe,
        )

        async def _never_runs() -> None:
            await asyncio.sleep(999)

        # Must NOT raise OperationalError; must return the late success.
        result = conn._run_sync(_never_runs())
        assert result == 1234
