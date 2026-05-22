"""Pin: ``_call_client``'s ``BaseExceptionGroup`` arm splits out
``CancelledError`` / ``KeyboardInterrupt`` / ``SystemExit`` children
and re-raises them rather than silently wrapping them as
``DatabaseError``.

PEP 654 + the asyncio cancellation invariant require that
``CancelledError`` propagate or be explicitly acknowledged — they
must NOT be silently converted into ordinary ``Exception``
subclasses. Without the split, a user wrapping ``cur.execute(...)``
inside ``async with asyncio.TaskGroup()`` and cancelling the group
would observe the cancel as a ``DatabaseError`` and downstream retry
loops would keep firing instead of cancelling cleanly.

The arm wraps the ``Exception``-class remainder as ``DatabaseError``
(unchanged behaviour); the cancel-class partition propagates intact.
"""

from __future__ import annotations

import asyncio

import pytest

from dqliteclient.exceptions import OperationalError
from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import DatabaseError

pytestmark = pytest.mark.asyncio


async def test_call_client_cancellederror_only_group_propagates_as_group() -> None:
    """A group whose only child is ``CancelledError`` propagates as
    a group containing the cancel (NOT as ``DatabaseError``).
    """

    async def _raise_cancel_group() -> None:
        raise BaseExceptionGroup("cancel-only", [asyncio.CancelledError()])

    with pytest.raises(BaseExceptionGroup) as excinfo:
        await _call_client(_raise_cancel_group())
    # The returned group contains the cancel — not wrapped as Error.
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)


async def test_call_client_mixed_group_propagates_cancel_and_drops_remainder() -> None:
    """A group with both CancelledError and OperationalError children:
    the cancel propagates (as its own group); the remainder is NOT
    re-raised because ``raise cancel_group`` exits the arm first.
    """

    async def _raise_mixed_group() -> None:
        raise BaseExceptionGroup(
            "mixed",
            [asyncio.CancelledError(), OperationalError("transport down", code=1)],
        )

    with pytest.raises(BaseExceptionGroup) as excinfo:
        await _call_client(_raise_mixed_group())
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)
    # The original group is still recoverable via __context__ /
    # __cause__ for diagnostic walks; the cancel partition is what
    # propagates as the active raise.


async def test_call_client_pure_exception_group_wraps_as_databaseerror() -> None:
    """A group with no cancel-class children is wrapped as
    ``DatabaseError`` per PEP 249 §7 — unchanged behaviour.
    """

    async def _raise_exc_group() -> None:
        raise BaseExceptionGroup(
            "transport-multi",
            [OperationalError("a", code=1), OperationalError("b", code=1)],
        )

    with pytest.raises(DatabaseError) as excinfo:
        await _call_client(_raise_exc_group())
    # The remainder remains accessible on __cause__ for SA's
    # walk_cause_chain disconnect classifier.
    assert isinstance(excinfo.value.__cause__, BaseExceptionGroup)


async def test_call_client_keyboardinterrupt_in_group_propagates() -> None:
    """KeyboardInterrupt is a BaseException subclass and must be
    treated identically to CancelledError.
    """

    async def _raise_ki_group() -> None:
        raise BaseExceptionGroup("ki-only", [KeyboardInterrupt()])

    with pytest.raises(BaseExceptionGroup) as excinfo:
        await _call_client(_raise_ki_group())
    inner = excinfo.value.exceptions
    assert any(isinstance(c, KeyboardInterrupt) for c in inner)


async def test_call_client_systemexit_in_group_propagates() -> None:
    """SystemExit follows the same discipline as CancelledError /
    KeyboardInterrupt.
    """

    async def _raise_se_group() -> None:
        raise BaseExceptionGroup("se-only", [SystemExit(1)])

    with pytest.raises(BaseExceptionGroup) as excinfo:
        await _call_client(_raise_se_group())
    inner = excinfo.value.exceptions
    assert any(isinstance(c, SystemExit) for c in inner)
