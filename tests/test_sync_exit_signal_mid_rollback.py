"""Pin: sync ``Connection.__exit__`` re-raises ``KeyboardInterrupt``
or ``SystemExit`` from ``rollback()`` after a body exception, with
a DEBUG breadcrumb log.

The async sibling has a parallel pin
(``test_aexit_rollback_debug_log.py``); the sync side previously
had no test for this specific arm. A future refactor that
broadened the catch to ``except BaseException`` (or removed the
breadcrumb) would silently swallow signal-interrupted rollback.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from dqlitedbapi import Connection


def _connection() -> Connection:
    conn = Connection("127.0.0.1:9001")
    # The early-return at the top of ``__exit__`` short-circuits if
    # ``_async_conn`` is None (no connect() called yet). Set a
    # sentinel so the rollback arm we're testing is reached.
    conn._async_conn = object()  # type: ignore[assignment]
    return conn


class TestSyncExitSignalDuringRollback:
    def test_keyboard_interrupt_propagates_with_debug_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        conn = _connection()
        body_exc = RuntimeError("body raised")
        with (
            patch.object(Connection, "rollback", side_effect=KeyboardInterrupt()),
            caplog.at_level(logging.DEBUG, logger="dqlitedbapi.connection"),
            pytest.raises(KeyboardInterrupt),
        ):
            conn.__exit__(
                type(body_exc),
                body_exc,
                body_exc.__traceback__,
            )
        debug_records = [
            r
            for r in caplog.records
            if r.levelno == logging.DEBUG and "rollback interrupted by signal" in r.message
        ]
        assert debug_records, (
            f"expected a DEBUG breadcrumb about signal-interrupted "
            f"rollback; got {[(r.levelno, r.message) for r in caplog.records]!r}"
        )

    def test_system_exit_propagates_with_debug_log(self, caplog: pytest.LogCaptureFixture) -> None:
        conn = _connection()
        body_exc = RuntimeError("body raised")
        with (
            patch.object(Connection, "rollback", side_effect=SystemExit(1)),
            caplog.at_level(logging.DEBUG, logger="dqlitedbapi.connection"),
            pytest.raises(SystemExit),
        ):
            conn.__exit__(
                type(body_exc),
                body_exc,
                body_exc.__traceback__,
            )
        debug_records = [
            r
            for r in caplog.records
            if r.levelno == logging.DEBUG and "rollback interrupted by signal" in r.message
        ]
        assert debug_records
