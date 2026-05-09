"""Pin: ``_RESOLVE_LEADER_CACHE_LOCK`` is replaced by a fresh
``threading.Lock`` in the child process via an
``os.register_at_fork(after_in_child=...)`` hook so a parent that
forked while another thread held the lock cannot leave the child
with a permanently-inherited held lock (deadlock).

Realistic threat: the dbapi sync layer spins up a daemon
``_loop_thread`` per ``Connection``, so any process opening a sync
connection then forking is multi-threaded at the moment of fork.
A parent thread parked inside ``_get_resolve_leader_cluster``'s
lock-protected composite would hand the child a held lock that
no thread in the child owns.
"""

from __future__ import annotations

import threading
from unittest import mock

import dqlitedbapi.connection as _conn_mod


def test_atfork_callback_replaces_cache_lock() -> None:
    """Calling the after-fork hook directly should swap the module-
    level lock for a fresh ``threading.Lock`` instance."""
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
        # The replacement must be acquirable (not held).
        assert replacement.acquire(blocking=False)
        replacement.release()
    finally:
        _conn_mod._RESOLVE_LEADER_CACHE_LOCK = original


def test_atfork_hook_registered_with_register_at_fork() -> None:
    """The module's after-fork callback must be registered via
    ``os.register_at_fork``. Without registration, the hook is dead
    code — verify by patching ``os.register_at_fork`` and re-importing
    the module to confirm the registration is hit on import."""
    import importlib
    import os

    register_calls: list[dict[str, object]] = []

    def fake_register_at_fork(**kwargs: object) -> None:
        register_calls.append(kwargs)

    with mock.patch.object(os, "register_at_fork", side_effect=fake_register_at_fork):
        importlib.reload(_conn_mod)

    # The module re-import should have called register_at_fork at
    # least twice (once for the existing _refresh_pid_cache callback
    # if any imported by client side, plus our new lock-replace).
    # We focus on the lock-replace call specifically:
    matching = [
        c
        for c in register_calls
        if c.get("after_in_child") is _conn_mod._at_fork_replace_resolve_leader_cache_lock
    ]
    assert matching, (
        "_at_fork_replace_resolve_leader_cache_lock must be registered "
        "via os.register_at_fork(after_in_child=...) so the child "
        "process inherits a fresh lock instead of a potentially-held one"
    )
