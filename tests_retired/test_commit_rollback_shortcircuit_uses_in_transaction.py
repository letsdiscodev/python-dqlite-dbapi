"""Pin the dbapi commit()/rollback() short-circuit reads ``in_transaction`` and only that.

The client property already ORs in ``_has_untracked_savepoint``, so the dbapi must
not re-peek the private attr; it reads the public property via a defensive getattr.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.connection import Connection


def _make_inner(in_transaction: bool, has_untracked: bool) -> Any:
    """Stub of DqliteConnection's short-circuit surface (the public property + private flag)."""
    inner = type("_FakeInner", (), {})()
    inner.in_transaction = in_transaction
    inner._has_untracked_savepoint = has_untracked
    inner.execute = AsyncMock(return_value=None)
    return inner


@pytest.mark.parametrize(
    ("in_transaction", "expect_wire_call"),
    [
        (False, False),  # no transaction → no wire round-trip
        (True, True),  # active transaction → COMMIT/ROLLBACK over the wire
    ],
)
def test_sync_commit_short_circuits_on_in_transaction(
    in_transaction: bool, expect_wire_call: bool
) -> None:
    run_sync_calls: list[object] = []

    def stub_run_sync(coro: object) -> None:
        run_sync_calls.append(coro)
        # Close the coro to avoid an unawaited-coroutine ResourceWarning.
        coro.close()  # type: ignore[attr-defined]

    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = _make_inner(in_transaction=in_transaction, has_untracked=False)
    conn.messages = []
    conn._check_thread = lambda: None
    conn._run_sync = stub_run_sync  # type: ignore[assignment]

    conn.commit()

    assert len(run_sync_calls) == (1 if expect_wire_call else 0)


@pytest.mark.parametrize(
    ("in_transaction", "expect_wire_call"),
    [
        (False, False),
        (True, True),
    ],
)
async def test_aio_commit_short_circuits_on_in_transaction(
    in_transaction: bool, expect_wire_call: bool
) -> None:
    conn = AsyncConnection("localhost:9001")
    conn._async_conn = _make_inner(in_transaction=in_transaction, has_untracked=False)

    await conn.commit()

    inner_execute: AsyncMock = conn._async_conn.execute  # type: ignore[assignment]
    assert inner_execute.await_count == (1 if expect_wire_call else 0)


@pytest.mark.parametrize(
    ("in_transaction", "expect_wire_call"),
    [
        (False, False),
        (True, True),
    ],
)
async def test_aio_rollback_short_circuits_on_in_transaction(
    in_transaction: bool, expect_wire_call: bool
) -> None:
    conn = AsyncConnection("localhost:9001")
    conn._async_conn = _make_inner(in_transaction=in_transaction, has_untracked=False)

    await conn.rollback()

    inner_execute: AsyncMock = conn._async_conn.execute  # type: ignore[assignment]
    assert inner_execute.await_count == (1 if expect_wire_call else 0)


async def test_aio_commit_routes_via_property_when_only_untracked_flag_set() -> None:
    """An untracked-savepoint-only tx still surfaces via the property's OR; the dbapi
    trusts the property and issues the wire COMMIT rather than re-computing it."""
    conn = AsyncConnection("localhost:9001")
    conn._async_conn = _make_inner(in_transaction=True, has_untracked=True)

    await conn.commit()

    inner_execute: AsyncMock = conn._async_conn.execute  # type: ignore[assignment]
    assert inner_execute.await_count == 1


async def test_aio_commit_mock_without_untracked_attr_short_circuits_cleanly() -> None:
    """Mock tolerance: a stub lacking ``_has_untracked_savepoint`` must still
    short-circuit, since the dbapi no longer peeks that private attr."""
    conn = AsyncConnection("localhost:9001")
    inner = type("_MinimalInner", (), {})()
    inner.in_transaction = False
    inner.execute = AsyncMock(return_value=None)
    conn._async_conn = inner

    await conn.commit()

    assert inner.execute.await_count == 0


async def test_aio_commit_mock_completely_missing_in_transaction_short_circuits() -> None:
    """A stub missing ``in_transaction`` itself must short-circuit via the
    ``getattr(..., False)`` default, not raise AttributeError on a bare read."""
    conn = AsyncConnection("localhost:9001")
    inner = type("_VeryMinimalInner", (), {})()
    inner.execute = AsyncMock(return_value=None)
    conn._async_conn = inner

    await conn.commit()

    assert inner.execute.await_count == 0
