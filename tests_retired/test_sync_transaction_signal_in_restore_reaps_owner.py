"""A signal landing in transaction()'s COMMIT/ROLLBACK owner-restore window leaves the slot
parked at the internal sentinel; the outer finally must reap it, else every later
``with conn.transaction()`` is permanently rejected as nested.
"""

from __future__ import annotations

import os
import threading
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _bare_connection() -> Connection:
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = MagicMock()  # truthy but unused; cursor() is mocked
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._transaction_owner = None
    conn.messages = []
    return conn


class _LockRaisingOnNthEnter:
    """A lock proxy that raises KeyboardInterrupt on the Nth ``__enter__`` (simulating a
    signal delivered between bytecodes), delegating to a real lock otherwise."""

    def __init__(self, n: int) -> None:
        self._n = n
        self._count = 0
        self._real = threading.Lock()

    def __enter__(self) -> None:
        self._count += 1
        if self._count == self._n:
            raise KeyboardInterrupt("signal in owner-restore window")
        self._real.acquire()

    def __exit__(self, *exc: object) -> None:
        if self._real.locked():
            self._real.release()


def test_signal_in_commit_restore_reaps_owner_slot() -> None:
    conn = _bare_connection()
    # COMMIT (clean) path acquires _state_lock 3x: reserve, park-sentinel, restore-token.
    # Raise on the 3rd (restore) so the slot is left at the parked sentinel.
    conn._state_lock = _LockRaisingOnNthEnter(3)  # type: ignore[assignment]
    conn.cursor = MagicMock(return_value=MagicMock())

    with pytest.raises(KeyboardInterrupt), conn.transaction():
        pass  # clean exit -> COMMIT path

    # The outer finally must have reaped the leaked sentinel; without that, the slot
    # stays non-None and transaction() is permanently wedged.
    assert conn._transaction_owner is None

    # A subsequent transaction() must NOT be rejected as nested.
    conn._state_lock = threading.Lock()
    conn.cursor = MagicMock(return_value=MagicMock())
    with conn.transaction():
        pass
    assert conn._transaction_owner is None
