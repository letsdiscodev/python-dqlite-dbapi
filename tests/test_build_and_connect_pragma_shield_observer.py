"""Pin: ``_build_and_connect``'s PRAGMA query_only cleanup arm
hoists ``conn.close()`` into an explicit Task with a done-callback
observer BEFORE shielding so an outer cancel landing mid-await does
not orphan an implicit Task.

The orphan would surface as ``Task exception was never retrieved``
at GC if ``close()`` later raised an Exception with no awaiter
holding the future. Same regression class as the prior bare-coro
shield sweep; this site was introduced by the ``session_mode="read_only"``
commit AFTER that sweep landed.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_pragma_cleanup_shield_uses_observer_not_bare_coro() -> None:
    """If the PRAGMA query_only emit raises, ``_build_and_connect``
    must hoist the cleanup ``conn.close()`` into an explicit Task
    + observer before shielding."""
    received_types: list[type] = []
    original_shield = asyncio.shield

    def recording_shield(arg, *args, **kwargs):
        received_types.append(type(arg))
        return original_shield(arg, *args, **kwargs)

    # Stub the connect path: produce a fake ``conn`` whose
    # ``execute`` raises (forcing the PRAGMA cleanup arm) and whose
    # ``close`` is awaitable. Patch ``DqliteConnection`` so the
    # ``_build_and_connect`` instantiation hands back our fake.
    fake_conn = MagicMock()
    fake_conn.connect = AsyncMock(return_value=None)
    fake_conn.execute = AsyncMock(side_effect=RuntimeError("PRAGMA failed"))
    fake_conn.close = AsyncMock(return_value=None)

    from dqlitedbapi import connection as _conn_mod

    def fake_dqlite_connection(*args: object, **kwargs: object) -> MagicMock:
        return fake_conn

    with (
        patch("dqlitedbapi.connection.DqliteConnection", new=fake_dqlite_connection),
        patch("dqlitedbapi.connection._resolve_leader", new=AsyncMock(return_value="addr:9001")),
        patch("asyncio.shield", side_effect=recording_shield),
        pytest.raises(RuntimeError, match="PRAGMA failed"),
    ):
        await _conn_mod._build_and_connect(
            address="addr:9001",
            database=":memory:",
            timeout=5.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=0.5,
            max_message_size=None,
            dial_func=None,
            session_mode="read_only",
        )

    assert received_types, "asyncio.shield was not invoked in the cleanup arm"
    arg_type = received_types[0]
    assert issubclass(arg_type, asyncio.Future) or arg_type is asyncio.Task, (
        f"PRAGMA cleanup passed {arg_type.__name__} to asyncio.shield — "
        "expected an explicit Task/Future hoist via asyncio.ensure_future."
    )
