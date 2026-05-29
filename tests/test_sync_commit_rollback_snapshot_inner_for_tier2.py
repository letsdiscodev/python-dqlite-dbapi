"""Sync commit()/rollback() snapshot ``_async_conn`` to a local so a foreign-thread
``force_close_transport`` racing the probe (tier-2, where ``_check_thread`` short-circuits)
cannot produce a silent no-op against an invalidated connection."""

from __future__ import annotations

import inspect


def test_commit_snapshots_inner_to_local() -> None:
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.commit)
    assert "inner = self._async_conn" in source
    assert "getattr(inner," in source


def test_rollback_snapshots_inner_to_local() -> None:
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.rollback)
    assert "inner = self._async_conn" in source
    assert "getattr(inner," in source


def test_commit_rationale_no_longer_claims_no_async_race() -> None:
    """The stale "no async race window" rationale comment must be gone (tier-2 voids it)."""
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.commit)
    assert "no async race window" not in source, (
        "stale rationale comment must be removed; tier-2 makes the claim structurally wrong"
    )
