"""``hasattr(conn, "total_changes")`` must not raise. ``total_changes`` is a stdlib-parity stub
that raises ``NotSupportedError`` when called; it is a method (not a ``@property``) so getattr
returns the bound method without invoking it and hasattr stays True."""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import NotSupportedError


def test_hasattr_total_changes_sync_does_not_raise() -> None:
    conn = dqlitedbapi.Connection("localhost:9001", timeout=1.0)
    try:
        try:
            present = hasattr(conn, "total_changes")
        except NotSupportedError:
            pytest.fail(
                "hasattr() leaked NotSupportedError — total_changes must be a "
                "method-stub like the rest of the family, not a @property"
            )
        assert present is True
        # And calling the stub still raises (parens — method form).
        with pytest.raises(NotSupportedError, match="total_changes"):
            conn.total_changes()
    finally:
        conn._closed = True


def test_hasattr_total_changes_async_does_not_raise() -> None:
    """Async sibling: AsyncConnection's ``total_changes`` was also a ``@property`` outlier."""
    aconn = AsyncConnection("localhost:9001", timeout=1.0)
    try:
        present = hasattr(aconn, "total_changes")
    except NotSupportedError:
        pytest.fail(
            "hasattr() leaked NotSupportedError on AsyncConnection — "
            "total_changes must be a method-stub like the rest of the family"
        )
    assert present is True
    with pytest.raises(NotSupportedError, match="total_changes"):
        aconn.total_changes()
