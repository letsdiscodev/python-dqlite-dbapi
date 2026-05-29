"""Pin: sync ``Connection`` isolation_level/autocommit setters store on the sync
wrapper's slot only; they deliberately do NOT mirror to the inner ``AsyncConnection``
(mirroring across the loop-thread boundary would add ordering hazards). Update
alongside the ``INDEPENDENT`` callout in ``connection.py`` if this ever changes.
"""

from __future__ import annotations

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def _connection_with_inner() -> tuple[Connection, AsyncConnection]:
    """Sync Connection with an inner AsyncConnection (via ``__new__``) so the
    divergence is observable without wire-side side-effects."""
    conn = Connection("127.0.0.1:9999")
    inner = AsyncConnection.__new__(AsyncConnection)
    inner._closed = False
    # AsyncConnection stands in for the typed DqliteConnection; setters never
    # touch the inner, so the static-type mismatch is benign.
    conn._async_conn = inner  # type: ignore[assignment]
    return conn, inner


def test_isolation_level_setter_does_not_mirror_to_inner() -> None:
    """Sync setter writes its own slot only; inner getter stays at default ``None``."""
    conn, inner = _connection_with_inner()
    try:
        conn.isolation_level = "DEFERRED"
        assert conn.isolation_level == "DEFERRED"
        assert getattr(inner, "_isolation_level_value", None) is None
    finally:
        conn._closed = True


def test_autocommit_setter_does_not_mirror_to_inner() -> None:
    """Sync setter writes its own slot only; inner getter stays at default ``True``."""
    conn, inner = _connection_with_inner()
    try:
        conn.autocommit = -1  # LEGACY_TRANSACTION_CONTROL sentinel
        assert conn.autocommit == -1
        assert not hasattr(inner, "_autocommit_value")
    finally:
        conn._closed = True
