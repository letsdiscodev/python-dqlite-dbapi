"""session_mode="read_only" emits PRAGMA query_only=1, making the engine reject writes.

Other session modes do not emit the PRAGMA and writes succeed.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import OperationalError


@pytest.fixture
def _create_test_table(cluster_address: str) -> None:
    """Ensure the test table exists and is empty."""
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
    """Writes on a read_only connection raise OperationalError with primary code 8."""
    conn = dqlitedbapi.connect(cluster_address, session_mode="read_only")
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM _session_mode_pin")
        assert cur.fetchall() == []
        with pytest.raises(OperationalError) as exc_info:
            cur.execute("INSERT INTO _session_mode_pin (val) VALUES ('x')")
        # Primary code 8 = SQLITE_READONLY.
        assert (exc_info.value.code or 0) & 0xFF == 8
        cur.close()
    finally:
        conn.close()


@pytest.mark.integration
def test_immediate_session_allows_writes(cluster_address: str, _create_test_table: None) -> None:
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
    """session_mode="deferred" allows writes; only "read_only" emits the PRAGMA."""
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
    """The mode and its construction-time default are stored on the connection."""
    conn = dqlitedbapi.connect(cluster_address, session_mode="read_only")
    try:
        assert conn.session_mode == "read_only"
        assert conn.default_session_mode == "read_only"
    finally:
        conn.close()


@pytest.mark.integration
def test_invalid_session_mode_raises_at_construct(cluster_address: str) -> None:
    with pytest.raises(dqlitedbapi.ProgrammingError, match="Invalid session_mode"):
        dqlitedbapi.connect(cluster_address, session_mode="not_a_mode")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_read_only_session_rejects_writes_at_prepare(
    cluster_address: str, _create_test_table: None
) -> None:
    """Sync/async parity: the async path emits the same read-only PRAGMA."""
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
