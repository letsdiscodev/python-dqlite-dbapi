"""Pin: ``dqlitedbapi.connect`` (sync, async ``connect``, async
``aconnect``) accept the same no-op sentinel values for
``isolation_level`` and ``autocommit`` that the post-construction
setter accepts — symmetric surface for cross-driver porting code
that passes stdlib defaults through to the constructor.

Stdlib `sqlite3` accepts both forms symmetrically:

    >>> con = sqlite3.connect(":memory:", isolation_level=None)
    >>> con.isolation_level
    None

The dqlite driver's setter already accepts ``isolation_level=None``
and ``autocommit=True/-1`` as documented no-ops; the constructor
was the asymmetric surface. Both surfaces now agree.

Non-sentinel values (``isolation_level="DEFERRED"``,
``autocommit=False``) are still rejected as
``NotSupportedError`` on both surfaces.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import NotSupportedError


def test_sync_connect_accepts_isolation_level_none() -> None:
    """The no-op sentinel ``None`` is accepted at the constructor
    just like the setter."""
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


def test_sync_connect_accepts_autocommit_true() -> None:
    """The no-op sentinel ``True`` is accepted."""
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=True)
    _ctor.assert_called_once()


def test_sync_connect_accepts_autocommit_minus_one() -> None:
    """Stdlib's ``LEGACY_TRANSACTION_CONTROL`` sentinel (-1) is the
    other accepted no-op."""
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=-1)
    _ctor.assert_called_once()


def test_sync_connect_rejects_isolation_level_deferred() -> None:
    """A non-sentinel value (an actual stdlib mode) is still
    rejected."""
    with pytest.raises(NotSupportedError, match="isolation_level"):
        dqlitedbapi.connect("127.0.0.1:9001", isolation_level="DEFERRED")


def test_sync_connect_rejects_autocommit_false() -> None:
    """``False`` is not a no-op sentinel — rejected."""
    with pytest.raises(NotSupportedError, match="autocommit"):
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=False)


def test_async_connect_accepts_isolation_level_none() -> None:
    """Sibling on the async ``connect`` (the non-awaitable form)."""
    from dqlitedbapi.aio import connect as aio_connect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        aio_connect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


def test_async_connect_rejects_isolation_level_deferred() -> None:
    from dqlitedbapi.aio import connect as aio_connect

    with pytest.raises(NotSupportedError, match="isolation_level"):
        aio_connect("127.0.0.1:9001", isolation_level="DEFERRED")


# The awaitable ``aconnect`` is the recommended async entry-point
# for new asyncio callers; the sentinel-handling block in its body
# is a literal copy of ``connect``'s, and benefits from the same
# behavioural pin so a regression deleting either copy surfaces.


@pytest.mark.asyncio
async def test_aconnect_accepts_isolation_level_none() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_accepts_autocommit_true() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", autocommit=True)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_accepts_autocommit_minus_one() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", autocommit=-1)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_rejects_isolation_level_deferred() -> None:
    from dqlitedbapi.aio import aconnect

    with pytest.raises(NotSupportedError, match="isolation_level"):
        await aconnect("127.0.0.1:9001", isolation_level="DEFERRED")


@pytest.mark.asyncio
async def test_aconnect_rejects_autocommit_false() -> None:
    from dqlitedbapi.aio import aconnect

    with pytest.raises(NotSupportedError, match="autocommit"):
        await aconnect("127.0.0.1:9001", autocommit=False)
