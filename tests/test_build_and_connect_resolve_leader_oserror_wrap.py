"""Pin: ``_build_and_connect``'s ``_resolve_leader`` try block
catches ``OSError`` and surfaces it as ``OperationalError``,
symmetric with the sibling ``conn.connect()`` block's defence-in-
depth arm.

The in-tree ``find_leader`` wraps every per-probe ``OSError`` in
``_ProbeMiss`` and aggregates as ``ClusterError`` — so on the
happy path this arm is unreached. It exists for:

1. Custom ``NodeStore``s that raise ``OSError`` from
   ``get_nodes()`` (e.g. a file-backed YAML store with a missing
   file).
2. ``socket.gaierror`` from future DNS paths.
3. ``TimeoutError`` (an ``OSError`` subclass since Python 3.11)
   leaked from a misconfigured ``asyncio.wait_for`` inside a
   third-party ``cluster_factory``.

A regression that drops the arm (or that introduces a new
``OSError``-raising path inside ``_resolve_leader`` without
covering it) would propagate ``OSError`` past the dbapi PEP 249
boundary.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dqlitedbapi.connection import _build_and_connect
from dqlitedbapi.exceptions import OperationalError


async def _raise_oserror(*_args: object, **_kw: object) -> object:
    raise OSError("simulated NodeStore read failure")


@pytest.mark.asyncio
async def test_resolve_leader_oserror_wraps_as_operational_error() -> None:
    """A bare ``OSError`` escaping ``_resolve_leader`` surfaces as
    ``OperationalError`` with the canonical "Failed to find leader"
    prefix — symmetric with the post-construct sibling arm."""
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
    """``TimeoutError`` is an ``OSError`` subclass since Python 3.11;
    the arm catches it via the OSError base class. Pin the
    Python-3.11+ behaviour explicitly so a regression on the
    base-class chain (or a refactor that narrows the arm to e.g.
    ``ConnectionError``) is caught."""

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
