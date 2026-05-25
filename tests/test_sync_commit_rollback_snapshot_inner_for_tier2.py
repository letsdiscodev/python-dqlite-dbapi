"""Pin: sync ``Connection.commit()`` / ``rollback()`` snapshot
``_async_conn`` to a local at the top so a foreign-thread
``force_close_transport`` racing the probe sequence cannot
produce a silent no-op commit/rollback against an invalidated
connection.

The prior rationale comment claimed "no async race window —
``_check_thread`` makes the sync caller single-threaded relative
to itself"; this was structurally wrong under tier-2
(``check_same_thread=False``) because ``_check_thread`` short-
circuits and ``force_close_transport`` is documented foreign-
thread-callable.

Without the snapshot, the foreign-thread null between the
``_async_conn is None`` check and the ``in_transaction`` read
caused the method to silently return (``in_transaction`` False
because ``getattr(None, ...)`` returns the default), so the user
saw "commit succeeded" against a force-closed connection.
"""

from __future__ import annotations

import inspect


def test_commit_snapshots_inner_to_local() -> None:
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.commit)
    # The snapshot pattern: assign to a local AND use the local
    # in the post-check probes (not ``self._async_conn``).
    assert "inner = self._async_conn" in source
    assert "getattr(inner," in source


def test_rollback_snapshots_inner_to_local() -> None:
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.rollback)
    assert "inner = self._async_conn" in source
    assert "getattr(inner," in source


def test_commit_rationale_no_longer_claims_no_async_race() -> None:
    """The stale "no async race window — _check_thread makes the
    sync caller single-threaded" comment must be removed/updated.
    """
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.commit)
    assert "no async race window" not in source, (
        "stale rationale comment must be removed; tier-2 makes the claim structurally wrong"
    )
