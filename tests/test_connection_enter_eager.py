"""``Connection.__enter__`` eagerly connects to match ``AsyncConnection``.

A caller porting between ``with connect(...) as c:`` and
``async with aconnect(...) as c:`` previously saw different
fail-fast semantics: the async flavour raised at the ``async with``
line when the cluster was unreachable, while the sync flavour waited
for the body's first operation.

Sync ``__enter__`` now calls ``self.connect()`` too. A failure during
eager-enter runs ``close()`` before re-raising so partial state does
not leak (Python does not invoke ``__exit__`` when ``__enter__``
raises).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import OperationalError


def test_enter_calls_connect_on_entry() -> None:
    conn = Connection("localhost:9001", timeout=0.1)
    with patch.object(Connection, "connect") as mock_connect, conn:
        pass
    mock_connect.assert_called_once()
    conn.close()


def test_enter_failure_propagates_without_returning_connection() -> None:
    """If ``connect()`` raises, the ``with`` statement does not bind."""
    conn = Connection("localhost:9001", timeout=0.1)
    with (
        patch.object(Connection, "connect", side_effect=OperationalError("boom")),
        pytest.raises(OperationalError, match="boom"),
        conn,
    ):
        # Must never reach here.
        raise AssertionError("body must not run on connect failure")
    # Partial-state cleanup ran: connection is closed.
    assert conn._closed
