"""``_join_budget_for_current_thread`` must survive interpreter shutdown,
where ``Py_FinalizeEx`` sets module globals (including ``asyncio``) to
``None``, without raising into the calling ``weakref.finalize`` machinery.
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
    """With the module-level ``asyncio`` set to ``None``, the helper must not
    raise and must fall back to the off-loop budget."""
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
    """An object that raises on attribute access must not propagate."""

    class _Broken:
        def __getattr__(self, name: str) -> Any:
            raise TypeError(f"phase-3 teardown sentinel for {name}")

    monkeypatch.setattr(_conn_mod, "asyncio", _Broken())

    budget = _conn_mod._join_budget_for_current_thread(0.5)
    assert budget == max(0.5, _conn_mod._LOOP_THREAD_JOIN_MIN_SECONDS)


def test_helper_returns_foreign_floor_on_loop_thread() -> None:
    """From a coroutine the helper returns the foreign-loop floor (the user
    loop is not parked)."""

    async def runner() -> float:
        return _conn_mod._join_budget_for_current_thread(0.5)

    budget = asyncio.run(runner())
    assert budget == _conn_mod._LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS


def test_helper_returns_full_budget_off_loop() -> None:
    """From a thread with no running loop the helper returns
    ``max(close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS)``."""
    result: list[float] = []

    def runner() -> None:
        result.append(_conn_mod._join_budget_for_current_thread(0.5))

    t = threading.Thread(target=runner)
    t.start()
    t.join()
    assert result == [max(0.5, _conn_mod._LOOP_THREAD_JOIN_MIN_SECONDS)]
