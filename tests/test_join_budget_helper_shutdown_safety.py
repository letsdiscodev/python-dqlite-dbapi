"""Pin: ``_join_budget_for_current_thread`` survives the
``Py_FinalizeEx`` phase-3 module-globals-set-to-None teardown
without raising into the calling ``weakref.finalize`` machinery.

During interpreter shutdown CPython's ``PyImport_Cleanup`` walks
``sys.modules`` and sets every module's globals to ``None``. If
``dqlitedbapi.connection.asyncio`` becomes ``None`` between the
finalize-callback firing and the helper's `asyncio.get_running_loop()`
dereference, the call would raise ``AttributeError`` —
``contextlib.suppress(RuntimeError)`` at the only protected call
site does NOT catch ``AttributeError``, and the second call site
inside ``force_close_transport`` has no suppression at all.

The fix captures ``asyncio`` as a kwarg-default at function-
definition time, mirroring the discipline ``_cleanup_loop_thread``
applies to ``warnings`` / ``logger`` / ``contextlib`` /
``sanitize_for_log``.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from dqlitedbapi import connection as _conn_mod


def test_helper_returns_off_loop_budget_when_asyncio_module_global_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulate phase-3 teardown: set the module-level ``asyncio``
    reference to ``None``. The helper must not raise; it must fall
    back to the off-loop budget so the surrounding ``thread.join``
    still runs."""
    monkeypatch.setattr(_conn_mod, "asyncio", None)

    budget = _conn_mod._join_budget_for_current_thread(0.5)

    expected = max(0.5, _conn_mod._LOOP_THREAD_JOIN_MIN_SECONDS)
    assert budget == expected, (
        f"helper returned {budget}; expected the off-loop budget "
        f"({expected}) when asyncio module-global is None"
    )


def test_helper_returns_off_loop_budget_when_asyncio_is_corrupted_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defence in depth: a ``mock``-style object that raises on
    attribute access must not propagate from the helper.
    """

    class _Broken:
        def __getattr__(self, name: str) -> Any:
            raise TypeError(f"phase-3 teardown sentinel for {name}")

    monkeypatch.setattr(_conn_mod, "asyncio", _Broken())

    budget = _conn_mod._join_budget_for_current_thread(0.5)
    assert budget == max(0.5, _conn_mod._LOOP_THREAD_JOIN_MIN_SECONDS)


def test_helper_returns_foreign_floor_on_loop_thread() -> None:
    """Positive case: when called from a coroutine the helper must
    return the foreign-loop floor so the user loop is not parked.
    """

    async def runner() -> float:
        return _conn_mod._join_budget_for_current_thread(0.5)

    budget = asyncio.run(runner())
    assert budget == _conn_mod._LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS


def test_helper_returns_full_budget_off_loop() -> None:
    """Positive case: from a thread with no running loop the helper
    must return ``max(close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS)``.
    """
    result: list[float] = []

    def runner() -> None:
        result.append(_conn_mod._join_budget_for_current_thread(0.5))

    t = threading.Thread(target=runner)
    t.start()
    t.join()
    assert result == [max(0.5, _conn_mod._LOOP_THREAD_JOIN_MIN_SECONDS)]
