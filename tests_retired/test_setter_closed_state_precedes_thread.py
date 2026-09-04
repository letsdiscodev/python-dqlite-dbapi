"""All state-mutating setters check closed-state BEFORE thread affinity (matching
row_factory/text_factory and the async surface), so a foreign-thread call on a CLOSED
connection raises InterfaceError (closed), not ProgrammingError (thread)."""

from __future__ import annotations

import threading
from typing import Any

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("autocommit", True),
        ("isolation_level", None),
    ],
)
def test_setter_on_closed_from_foreign_thread_raises_closed_first(name: str, value: Any) -> None:
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
    assert isinstance(captured[0], InterfaceError), (
        f"{name}.setter on a closed conn (even from a foreign thread) "
        f"must raise InterfaceError (closed-state precedence); "
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
