"""Pin: ``Connection(session_mode="read_only")`` causes the dqlite
engine to refuse writes at PREPARE.

The ``_build_and_connect`` hook emits ``PRAGMA query_only = 1`` on
the live wire connection (post-handshake) before publishing it. The
engine then rejects every INSERT / UPDATE / DELETE / CREATE / DROP
at PREPARE with ``SQLITE_READONLY (primary code 8)``.

Other session modes (``"immediate"``, ``"deferred"``, ``"exclusive"``)
do NOT emit the PRAGMA and writes succeed normally.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import OperationalError


@pytest.fixture
def _create_test_table(cluster_address: str) -> None:
    """Ensure the test table exists; clean any leftover rows."""
    conn = dqlitedbapi.connect(cluster_address)
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS _session_mode_pin (id INTEGER PRIMARY KEY, val TEXT)"
        )
        cur.execute("DELETE FROM _session_mode_pin")
        conn.commit()
        cur.close()
    finally:
        conn.close()


@pytest.mark.integration
def test_read_only_session_rejects_writes_at_prepare(
    cluster_address: str, _create_test_table: None
) -> None:
    """Writes against a ``session_mode="read_only"`` connection raise
    ``OperationalError`` with primary code 8 (SQLITE_READONLY)."""
    conn = dqlitedbapi.connect(cluster_address, session_mode="read_only")
    try:
        cur = conn.cursor()
        # Reads succeed.
        cur.execute("SELECT id FROM _session_mode_pin")
        assert cur.fetchall() == []
        # Writes raise SQLITE_READONLY.
        with pytest.raises(OperationalError) as exc_info:
            cur.execute("INSERT INTO _session_mode_pin (val) VALUES ('x')")
        # Primary code 8 = SQLITE_READONLY. The error message is the
        # engine's own ``attempt to write a readonly database`` —
        # we surface it verbatim per the WIP design doc decision.
        assert (exc_info.value.code or 0) & 0xFF == 8
        cur.close()
    finally:
        conn.close()


@pytest.mark.integration
def test_immediate_session_allows_writes(cluster_address: str, _create_test_table: None) -> None:
    """Default ``session_mode="immediate"`` allows writes."""
    conn = dqlitedbapi.connect(cluster_address)
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO _session_mode_pin (val) VALUES ('y')")
        conn.commit()
        cur.close()
    finally:
        conn.close()


@pytest.mark.integration
def test_deferred_session_allows_writes(cluster_address: str, _create_test_table: None) -> None:
    """``session_mode="deferred"`` allows writes — only ``"read_only"``
    emits the PRAGMA."""
    conn = dqlitedbapi.connect(cluster_address, session_mode="deferred")
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO _session_mode_pin (val) VALUES ('z')")
        conn.commit()
        cur.close()
    finally:
        conn.close()


@pytest.mark.integration
def test_session_mode_stored_on_connection(cluster_address: str) -> None:
    """The two attributes are present on the connection and the
    construction-time default is captured in
    ``_dqlite_session_mode_default``."""
    conn = dqlitedbapi.connect(cluster_address, session_mode="read_only")
    try:
        assert conn._dqlite_session_mode == "read_only"
        assert conn._dqlite_session_mode_default == "read_only"
    finally:
        conn.close()


@pytest.mark.integration
def test_invalid_session_mode_raises_at_construct(cluster_address: str) -> None:
    with pytest.raises(ValueError, match="Invalid session_mode"):
        dqlitedbapi.connect(cluster_address, session_mode="not_a_mode")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_read_only_session_rejects_writes_at_prepare(
    cluster_address: str, _create_test_table: None
) -> None:
    """Pin sync/async parity: the async path goes through the same
    ``_build_and_connect`` PRAGMA emit, but pin it directly so a
    future refactor splitting the sync/async paths would surface a
    regression here rather than in unrelated callers."""
    from dqlitedbapi.aio import aconnect

    conn = await aconnect(cluster_address, session_mode="read_only")
    try:
        cur = conn.cursor()
        await cur.execute("SELECT id FROM _session_mode_pin")
        assert await cur.fetchall() == []
        with pytest.raises(OperationalError) as exc_info:
            await cur.execute("INSERT INTO _session_mode_pin (val) VALUES ('async-x')")
        assert (exc_info.value.code or 0) & 0xFF == 8
        cur.close()
    finally:
        await conn.close()
