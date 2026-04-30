"""Pin: PEP 249 optional-extension stubs are exposed as
always-raising methods (PEP 249 §7 + cross-driver
``except dbapi.Error:`` discipline). This means ``hasattr``
returns True against this driver, diverging from stdlib
``sqlite3`` (which omits ``tpc_*`` / ``callproc`` / ``nextset``
/ ``scroll`` entirely).

The divergence is documented in the relevant module / method
docstrings. Cross-driver code porting from stdlib must use
``try/except NotSupportedError`` for feature detection,
not ``hasattr``. This test pins the documented contract so a
future "harmonise hasattr with stdlib" change has to flip the
test deliberately.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import NotSupportedError


@pytest.fixture
def conn() -> Connection:
    return Connection("localhost:9001", timeout=1.0)


@pytest.mark.parametrize(
    "name",
    [
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
    ],
)
def test_connection_stub_methods_present_for_pep249_compliance(conn: Connection, name: str) -> None:
    """``hasattr`` returns True — the stub is present so
    ``except dbapi.Error:`` catches the rejection uniformly.
    Stdlib ``sqlite3`` omits these (so ``hasattr`` is False
    there). Documented divergence."""
    assert hasattr(conn, name)
    method = getattr(conn, name)
    assert callable(method)


def test_connection_tpc_methods_raise_not_supported(conn: Connection) -> None:
    """Stubs raise ``NotSupportedError`` (a ``dbapi.Error``
    subclass) so cross-driver ``except dbapi.Error:`` catches.
    The ``hasattr`` trap is the cost; the catch-uniformity is
    the benefit. Pin both halves."""
    with pytest.raises(NotSupportedError, match="two-phase commit"):
        conn.tpc_begin(object())


def test_cursor_callproc_nextset_scroll_present_but_raise() -> None:
    """Cursor stubs match the connection-side discipline:
    present + raise NotSupportedError. ``hasattr`` is True;
    the ``try/except`` portable path produces the right answer."""
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
        conn._closed = True
