"""Pin: ``_resolve_leader`` OSError surfaces as OperationalError, not past the PEP 249
boundary (custom NodeStores, DNS gaierror, or leaked TimeoutError on the unreached arm)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dqlitedbapi.connection import _build_and_connect
from dqlitedbapi.exceptions import OperationalError


async def _raise_oserror(*_args: object, **_kw: object) -> object:
    raise OSError("simulated NodeStore read failure")


@pytest.mark.asyncio
async def test_resolve_leader_oserror_wraps_as_operational_error() -> None:
    with (
        patch("dqlitedbapi.connection._resolve_leader", new=_raise_oserror),
        pytest.raises(OperationalError, match="Failed to find leader") as info,
    ):
        await _build_and_connect(
            "127.0.0.1:9001",
            database="default",
            timeout=2.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=1.0,
        )

    assert "simulated NodeStore read failure" in str(info.value)
    assert isinstance(info.value.__cause__, OSError)


@pytest.mark.asyncio
async def test_resolve_leader_timeouterror_wraps_as_operational_error() -> None:
    """TimeoutError (an OSError subclass since 3.11) is caught via the OSError base."""

    async def _raise_timeout(*_a: object, **_kw: object) -> object:
        raise TimeoutError("simulated asyncio.wait_for")

    with (
        patch("dqlitedbapi.connection._resolve_leader", new=_raise_timeout),
        pytest.raises(OperationalError, match="Failed to find leader"),
    ):
        await _build_and_connect(
            "127.0.0.1:9001",
            database="default",
            timeout=2.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=1.0,
        )
