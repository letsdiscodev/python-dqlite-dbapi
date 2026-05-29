"""Pin: the connect-path ``BaseExceptionGroup`` arms split out cancel-class children
(CancelledError/KeyboardInterrupt/SystemExit) and re-raise them rather than wrapping
as OperationalError. Mirrors the ``_call_client`` cancel-class split.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractContextManager
from unittest.mock import AsyncMock, patch

import pytest

from dqliteclient.exceptions import (
    DqliteConnectionError,
)
from dqliteclient.exceptions import (
    OperationalError as ClientOperationalError,
)
from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import OperationalError as DbapiOperationalError

pytestmark = pytest.mark.asyncio


def _patch_resolve_leader_raise(
    eg_factory: Callable[[], BaseExceptionGroup],
) -> AbstractContextManager[object]:
    """Patch ``_resolve_leader`` to raise the test's BaseExceptionGroup so the
    connect-path arms own the response."""
    return patch(
        "dqlitedbapi.connection._resolve_leader",
        AsyncMock(side_effect=eg_factory()),
    )


def _cancel_only_group() -> BaseExceptionGroup:
    return BaseExceptionGroup("cancel-only", [asyncio.CancelledError()])


def _mixed_group() -> BaseExceptionGroup:
    return BaseExceptionGroup(
        "mixed",
        [
            asyncio.CancelledError(),
            ClientOperationalError("transport down", code=1),
        ],
    )


def _pure_exception_group() -> BaseExceptionGroup:
    return BaseExceptionGroup(
        "transport-multi",
        [
            ClientOperationalError("a", code=1),
            DqliteConnectionError("connection refused"),
        ],
    )


async def test_aconnect_cancel_only_group_propagates_as_group() -> None:
    """A cancel-only group must re-raise the cancel, not wrap as OperationalError."""
    with _patch_resolve_leader_raise(_cancel_only_group):
        with pytest.raises(BaseExceptionGroup) as excinfo:
            await aconnect("localhost:9001", timeout=2.0)
        inner = excinfo.value.exceptions
        assert any(isinstance(c, asyncio.CancelledError) for c in inner)


async def test_aconnect_mixed_group_propagates_cancel_partition() -> None:
    """Cancel partition wins over the Exception remainder; the cancel propagates."""
    with _patch_resolve_leader_raise(_mixed_group):
        with pytest.raises(BaseExceptionGroup) as excinfo:
            await aconnect("localhost:9001", timeout=2.0)
        inner = excinfo.value.exceptions
        assert any(isinstance(c, asyncio.CancelledError) for c in inner)


async def test_aconnect_pure_exception_group_wraps_as_operationalerror() -> None:
    """A group with no cancel-class children retains the OperationalError wrap (PEP 249 §7)."""
    with _patch_resolve_leader_raise(_pure_exception_group):
        with pytest.raises(DbapiOperationalError) as excinfo:
            await aconnect("localhost:9001", timeout=2.0)
        # __cause__ stays accessible for SQLAlchemy's walk_cause_chain classifier.
        assert isinstance(excinfo.value.__cause__, BaseExceptionGroup)
