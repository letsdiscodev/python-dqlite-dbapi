"""Pin: the connect-path ``BaseExceptionGroup`` arms in
``connection.py`` split out ``CancelledError`` / ``KeyboardInterrupt``
/ ``SystemExit`` children and re-raise them rather than silently
wrapping them as ``OperationalError``.

Mirrors the ``_call_client`` cancel-class split (see
``test_call_client_baseexceptiongroup_cancel_split.py``) for the
async connect (``aconnect``) and sync connect paths. The in-tree
primary raise path for ``BaseExceptionGroup`` is
``ConnectionPool.initialize`` (in dqliteclient); a future dbapi-side
pool or third-party retry middleware wrapping the connect coro could
route a group through one of the two arms.
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
    """Patch ``_resolve_leader`` (the find-leader entry point reached
    by the connect / aconnect paths) so it raises whichever
    ``BaseExceptionGroup`` the test provides. The
    ``BaseExceptionGroup`` arms in ``connection.py:644-660`` and
    ``connection.py:803-815`` then own the response.
    """
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
    """The find-leader layer raises a cancel-only group; the
    connect path's BaseExceptionGroup arm must re-raise the cancel
    rather than wrap as OperationalError.
    """
    with _patch_resolve_leader_raise(_cancel_only_group):
        with pytest.raises(BaseExceptionGroup) as excinfo:
            await aconnect("localhost:9001", timeout=2.0)
        inner = excinfo.value.exceptions
        assert any(isinstance(c, asyncio.CancelledError) for c in inner)


async def test_aconnect_mixed_group_propagates_cancel_partition() -> None:
    """Cancel partition wins over the Exception remainder — the
    cancel propagates and the wrapper exits before reaching the
    OperationalError wrap.
    """
    with _patch_resolve_leader_raise(_mixed_group):
        with pytest.raises(BaseExceptionGroup) as excinfo:
            await aconnect("localhost:9001", timeout=2.0)
        inner = excinfo.value.exceptions
        assert any(isinstance(c, asyncio.CancelledError) for c in inner)


async def test_aconnect_pure_exception_group_wraps_as_operationalerror() -> None:
    """A group with no cancel-class children retains the
    OperationalError wrap (PEP 249 §7).
    """
    with _patch_resolve_leader_raise(_pure_exception_group):
        with pytest.raises(DbapiOperationalError) as excinfo:
            await aconnect("localhost:9001", timeout=2.0)
        # The remainder remains accessible on __cause__ for SA's
        # walk_cause_chain disconnect classifier.
        assert isinstance(excinfo.value.__cause__, BaseExceptionGroup)
