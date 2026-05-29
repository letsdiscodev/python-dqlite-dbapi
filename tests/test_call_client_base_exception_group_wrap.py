"""Pin: ``_call_client`` wraps a ``BaseExceptionGroup`` as a PEP 249 DatabaseError.
The group is a BaseException (not Exception, per PEP 654) so no client/wire arm matches it."""

from __future__ import annotations

from typing import Any

import pytest

from dqliteclient import exceptions as _client_exc
from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import DatabaseError


@pytest.mark.asyncio
async def test_base_exception_group_wraps_as_database_error() -> None:
    """A group must surface as DatabaseError (group on __cause__) so ``dbapi.Error`` catches it."""

    async def aggregate_op() -> Any:
        raise BaseExceptionGroup(
            "synthetic aggregate",
            [
                _client_exc.DqliteConnectionError("seed-A down"),
                _client_exc.DqliteConnectionError("seed-B down"),
            ],
        )

    with pytest.raises(DatabaseError) as info:
        await _call_client(aggregate_op())

    err = info.value
    assert "aggregate" in str(err)
    # Python downgrades BaseExceptionGroup to ExceptionGroup when all children derive Exception.
    assert "ExceptionGroup" in str(err)
    assert "2 child" in str(err)
    # Original group survives on __cause__ so SA's _walk_cause_chain can descend the children.
    assert isinstance(err.__cause__, BaseExceptionGroup)
    assert len(err.__cause__.exceptions) == 2


@pytest.mark.asyncio
async def test_exception_group_subclass_also_wrapped() -> None:
    """ExceptionGroup inherits BaseExceptionGroup, so the same arm catches it."""

    async def task_group_failure() -> Any:
        raise ExceptionGroup(
            "TaskGroup-flavour",
            [_client_exc.OperationalError("boom", code=1)],
        )

    with pytest.raises(DatabaseError) as info:
        await _call_client(task_group_failure())

    assert "ExceptionGroup" in str(info.value)


@pytest.mark.asyncio
async def test_base_exception_group_with_single_child() -> None:
    """A single-child group still routes through the wrap; the message names the single class."""

    async def single_op() -> Any:
        raise BaseExceptionGroup(
            "single",
            [_client_exc.DqliteConnectionError("solo")],
        )

    with pytest.raises(DatabaseError) as info:
        await _call_client(single_op())

    assert "1 child" in str(info.value)
    assert "DqliteConnectionError" in str(info.value)
