"""autocommit/isolation_level setters check thread before closed-state, so a foreign-thread
call (even on a closed conn) raises ProgrammingError, not InterfaceError."""

from __future__ import annotations

import threading
from typing import Any

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("autocommit", True),
        ("isolation_level", None),
    ],
)
def test_setter_on_closed_from_foreign_thread_raises_thread_affinity(name: str, value: Any) -> None:
    c = dqlitedbapi.connect("127.0.0.1:9999")
    c._closed = True
    c._closed_flag[0] = True

    captured: list[BaseException] = []

    def worker() -> None:
        try:
            setattr(c, name, value)
        except BaseException as e:
            captured.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert len(captured) == 1
    assert isinstance(captured[0], ProgrammingError), (
        f"{name}.setter from a foreign thread (even on closed conn) "
        f"must raise ProgrammingError (thread-affinity precedence); "
        f"got {type(captured[0]).__name__}: {captured[0]}"
    )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("autocommit", True),
        ("isolation_level", None),
        ("text_factory", str),
    ],
)
def test_setter_clears_messages_first(name: str, value: Any) -> None:
    """Every public state-mutating method clears self.messages first (PEP 249 §6.4)."""
    c = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        c.messages.append((Exception, Exception("stale")))
        setattr(c, name, value)
        assert c.messages == []
    finally:
        c._closed_flag[0] = True
