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

import inspect
import re
import threading

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


def test_atfork_hook_registered_at_module_level() -> None:
    """The after-fork hook must be registered via
    ``os.register_at_fork(after_in_child=_at_fork_replace_resolve_leader_cache_lock)``
    at module top-level so the child process inherits a fresh lock
    instead of a potentially-held one.

    Verified by source-level regex (NOT by ``importlib.reload``).
    Reload mutates module state in-place: the ``Connection`` class
    identity changes, the original ``_RESOLVE_LEADER_CACHE`` /
    ``_RESOLVE_LEADER_CACHE_PID`` globals are reset, and the
    OS-level fork-handler list still holds the original function
    pointer. Subsequent tests in the same pytest session that rely
    on ``isinstance(x, dqlitedbapi.Connection)`` would fail
    spuriously. Source-level introspection avoids the pollution.
    """
    src = inspect.getsource(_conn_mod)
    pattern = (
        r"os\.register_at_fork\("
        r"\s*after_in_child=_at_fork_replace_resolve_leader_cache_lock"
        r"\s*\)"
    )
    if re.search(pattern, src) is None:
        raise AssertionError(
            "_at_fork_replace_resolve_leader_cache_lock must be "
            "registered via os.register_at_fork(after_in_child=...) "
            "so the child process inherits a fresh lock instead of a "
            "potentially-held one. Source-level regex did not match — "
            "check that the registration call is at module top-level "
            "and uses the literal kwarg name 'after_in_child'."
        )
