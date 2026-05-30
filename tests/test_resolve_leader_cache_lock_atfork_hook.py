"""``_RESOLVE_LEADER_CACHE_LOCK`` is replaced by a fresh lock after fork so a
child cannot inherit a held lock and deadlock."""

from __future__ import annotations

import threading

import dqlitedbapi.connection as _conn_mod


def test_atfork_callback_replaces_cache_lock() -> None:
    original = _conn_mod._RESOLVE_LEADER_CACHE_LOCK
    try:
        _conn_mod._at_fork_replace_resolve_leader_cache_lock()
        replacement = _conn_mod._RESOLVE_LEADER_CACHE_LOCK
        assert replacement is not original, (
            "after-fork hook must swap the module-level lock for a "
            "fresh instance so an inherited held lock cannot deadlock "
            "the child"
        )
        assert isinstance(replacement, type(threading.Lock()))
        assert replacement.acquire(blocking=False)
        replacement.release()
    finally:
        _conn_mod._RESOLVE_LEADER_CACHE_LOCK = original
