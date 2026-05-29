"""``_RESOLVE_LEADER_CACHE_LOCK`` is replaced by a fresh lock after fork so a
child cannot inherit a held lock and deadlock."""

from __future__ import annotations

import inspect
import re
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


def test_atfork_hook_registered_at_module_level() -> None:
    """Verify the at-fork registration via source regex, not importlib.reload:
    reload mutates module state and pollutes later tests in the session."""
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
