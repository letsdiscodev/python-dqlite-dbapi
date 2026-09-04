"""Pin: ``_call_client``'s group arm re-raises CancelledError/KeyboardInterrupt/SystemExit
children intact rather than wrapping them as DatabaseError (else cancels would mask and retry)."""

from __future__ import annotations

import asyncio

import pytest

from dqliteclient.exceptions import OperationalError
from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import DatabaseError

pytestmark = pytest.mark.asyncio


async def test_call_client_cancellederror_only_group_propagates_as_group() -> None:
    """A cancel-only group propagates as a group containing the cancel, not as DatabaseError."""

    async def _raise_cancel_group() -> None:
        raise BaseExceptionGroup("cancel-only", [asyncio.CancelledError()])

    with pytest.raises(BaseExceptionGroup) as excinfo:
        await _call_client(_raise_cancel_group())
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)


async def test_call_client_mixed_group_propagates_cancel_and_drops_remainder() -> None:
    """Mixed group: the cancel propagates first; the OperationalError remainder is dropped."""

    async def _raise_mixed_group() -> None:
        raise BaseExceptionGroup(
            "mixed",
            [asyncio.CancelledError(), OperationalError("transport down", code=1)],
        )

    with pytest.raises(BaseExceptionGroup) as excinfo:
        await _call_client(_raise_mixed_group())
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)


async def test_call_client_pure_exception_group_wraps_as_databaseerror() -> None:
    """A group with no cancel-class children is wrapped as DatabaseError per PEP 249 §7."""

    async def _raise_exc_group() -> None:
        raise BaseExceptionGroup(
            "transport-multi",
            [OperationalError("a", code=1), OperationalError("b", code=1)],
        )

    with pytest.raises(DatabaseError) as excinfo:
        await _call_client(_raise_exc_group())
    # Remainder stays on __cause__ for SA's walk_cause_chain disconnect classifier.
    assert isinstance(excinfo.value.__cause__, BaseExceptionGroup)


async def test_call_client_keyboardinterrupt_in_group_propagates() -> None:
    """KeyboardInterrupt (a BaseException) is treated identically to CancelledError."""

    async def _raise_ki_group() -> None:
        raise BaseExceptionGroup("ki-only", [KeyboardInterrupt()])

    with pytest.raises(BaseExceptionGroup) as excinfo:
        await _call_client(_raise_ki_group())
    inner = excinfo.value.exceptions
    assert any(isinstance(c, KeyboardInterrupt) for c in inner)


async def test_call_client_systemexit_in_group_propagates() -> None:
    """SystemExit follows the same discipline as CancelledError / KeyboardInterrupt."""

    async def _raise_se_group() -> None:
        raise BaseExceptionGroup("se-only", [SystemExit(1)])

    with pytest.raises(BaseExceptionGroup) as excinfo:
        await _call_client(_raise_se_group())
    inner = excinfo.value.exceptions
    assert any(isinstance(c, SystemExit) for c in inner)
