"""Optional-extension stubs are always-raising methods, so hasattr returns True.

Diverges from stdlib sqlite3 (which omits them); feature detection must use
try/except NotSupportedError, not hasattr.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import NotSupportedError


@pytest.fixture
def conn() -> Connection:
    return Connection("localhost:9001", timeout=1.0)


_STUB_NAMES = [
    "tpc_begin",
    "tpc_prepare",
    "tpc_commit",
    "tpc_rollback",
    "tpc_recover",
    "xid",
    "enable_load_extension",
    "load_extension",
    "backup",
    "iterdump",
    "create_function",
    "create_aggregate",
    "create_collation",
    "create_window_function",
    # total_changes is a method, not a @property, to keep the hasattr-True invariant.
    "total_changes",
]


@pytest.mark.parametrize("name", _STUB_NAMES)
def test_connection_stub_methods_present_for_pep249_compliance(conn: Connection, name: str) -> None:
    assert hasattr(conn, name)
    method = getattr(conn, name)
    assert callable(method)


@pytest.mark.parametrize("name", _STUB_NAMES)
def test_async_connection_stub_methods_present_for_pep249_compliance(name: str) -> None:
    from dqlitedbapi.aio.connection import AsyncConnection

    aconn = AsyncConnection("localhost:9001", timeout=1.0)
    assert hasattr(aconn, name)
    method = getattr(aconn, name)
    assert callable(method)


def test_connection_tpc_methods_raise_not_supported(conn: Connection) -> None:
    """Stubs raise NotSupportedError (a dbapi.Error subclass)."""
    with pytest.raises(NotSupportedError, match="two-phase commit"):
        conn.tpc_begin(object())


def test_cursor_callproc_nextset_scroll_present_but_raise() -> None:
    """Cursor stubs are present (hasattr True) but raise NotSupportedError."""
    conn = Connection("localhost:9001", timeout=1.0)
    cur = conn.cursor()
    try:
        for name in ("callproc", "nextset", "scroll"):
            assert hasattr(cur, name)

        with pytest.raises(NotSupportedError, match="stored procedures"):
            cur.callproc("foo")
        with pytest.raises(NotSupportedError, match="multiple result sets"):
            cur.nextset()
        with pytest.raises(NotSupportedError, match="not scrollable"):
            cur.scroll(0)
    finally:
        cur.close()
        conn.close()
