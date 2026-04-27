"""Pin the dbapi ``commit()`` / ``rollback()`` short-circuit reads
``in_transaction`` and ONLY ``in_transaction``.

The client-layer ``DqliteConnection.in_transaction`` already ORs in
``_has_untracked_savepoint``:

    @property
    def in_transaction(self) -> bool:
        return self._in_transaction or self._has_untracked_savepoint

So the dbapi's old guard

    if not self._async_conn.in_transaction and not getattr(
        self._async_conn, "_has_untracked_savepoint", False
    ):
        return

duplicated the OR — every input where the second clause would have
matched also matched ``in_transaction``. The cleanup uses one
defensive ``getattr`` on the public property:

    if not getattr(self._async_conn, "in_transaction", False):
        return

These tests pin the matrix so a future regression that swaps the
property read back to a private-attr peek (or that drops the
short-circuit entirely) surfaces as a failure.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.connection import Connection


def _make_inner(in_transaction: bool, has_untracked: bool) -> Any:
    """Build a stub matching ``DqliteConnection``'s public surface for
    the short-circuit check. ``in_transaction`` is the property the
    dbapi reads; ``_has_untracked_savepoint`` is included so a test
    that strips it (mock-tolerance check) can pass."""
    inner = type("_FakeInner", (), {})()
    inner.in_transaction = in_transaction
    inner._has_untracked_savepoint = has_untracked
    inner.execute = AsyncMock(return_value=None)
    return inner


# --- Sync wrapper ---------------------------------------------------


@pytest.mark.parametrize(
    ("in_transaction", "expect_wire_call"),
    [
        (False, False),  # quiet path: no transaction → no wire round-trip
        (True, True),  # active transaction → COMMIT/ROLLBACK over the wire
    ],
)
def test_sync_commit_short_circuits_on_in_transaction(
    in_transaction: bool, expect_wire_call: bool
) -> None:
    """The property is the single authoritative read. When it is
    ``False`` the dbapi must NOT call ``_run_sync`` (and thus must
    NOT issue a wire COMMIT). A sentinel-counting stub for
    ``_run_sync`` makes the contract directly observable."""
    run_sync_calls: list[object] = []

    def stub_run_sync(coro: object) -> None:
        run_sync_calls.append(coro)
        # Close the coroutine so we don't leak unawaited-coroutine
        # ResourceWarnings — the production path would await it.
        coro.close()  # type: ignore[attr-defined]

    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = _make_inner(in_transaction=in_transaction, has_untracked=False)
    conn.messages = []
    conn._check_thread = lambda: None
    conn._run_sync = stub_run_sync  # type: ignore[assignment]

    conn.commit()

    assert len(run_sync_calls) == (1 if expect_wire_call else 0)


# --- Async wrapper --------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("in_transaction", "expect_wire_call"),
    [
        (False, False),  # quiet path
        (True, True),  # wire COMMIT
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_aio_commit_routes_via_property_when_only_untracked_flag_set() -> None:
    """When only ``_has_untracked_savepoint`` is true (and the
    underlying ``_in_transaction`` is false) the client-layer property
    still returns True via its OR. The dbapi reads the property and
    issues the wire COMMIT — the autobegun server-side tx must be
    closed even though the dbapi never saw the BEGIN."""
    # Stub: ``in_transaction`` already reflects the OR, mirroring the
    # real client property. This pins the contract that the dbapi
    # trusts the property's OR rather than re-computing it.
    conn = AsyncConnection("localhost:9001")
    conn._async_conn = _make_inner(in_transaction=True, has_untracked=True)

    await conn.commit()

    inner_execute: AsyncMock = conn._async_conn.execute  # type: ignore[assignment]
    assert inner_execute.await_count == 1


@pytest.mark.asyncio
async def test_aio_commit_mock_without_untracked_attr_short_circuits_cleanly() -> None:
    """Mock tolerance: a stub that lacks ``_has_untracked_savepoint``
    entirely (older test fixtures, minimal stubs) must short-circuit
    via the ``getattr`` defensive read on ``in_transaction``. The old
    code had an explicit ``getattr`` for the same purpose; the cleanup
    moves it one level up to the public property."""
    conn = AsyncConnection("localhost:9001")
    inner = type("_MinimalInner", (), {})()
    inner.in_transaction = False
    inner.execute = AsyncMock(return_value=None)
    conn._async_conn = inner

    # Must not raise AttributeError on either ``_has_untracked_savepoint``
    # (we don't peek at it any more) or on the property itself
    # (``getattr`` defends against a stub that strips it).
    await conn.commit()

    assert inner.execute.await_count == 0


@pytest.mark.asyncio
async def test_aio_commit_mock_completely_missing_in_transaction_short_circuits() -> None:
    """Even a stub missing ``in_transaction`` itself must short-circuit
    (not raise AttributeError) — this is the layer of defence the
    cleanup explicitly preserves with the ``getattr(..., False)``
    default. Pins that we don't regress to a bare attribute read."""
    conn = AsyncConnection("localhost:9001")
    inner = type("_VeryMinimalInner", (), {})()
    inner.execute = AsyncMock(return_value=None)
    conn._async_conn = inner

    # No AttributeError — ``getattr(..., "in_transaction", False)`` returns False.
    await conn.commit()

    assert inner.execute.await_count == 0
