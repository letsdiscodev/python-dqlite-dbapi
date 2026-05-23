"""Pin: ``Connection.autocommit.setter``, ``Connection.isolation_level.setter``,
and ``Connection.text_factory.setter`` order ``_closed`` before
``_check_thread()``, so a cross-thread call on a closed connection
surfaces ``InterfaceError("Connection is closed")`` — the more salient
diagnostic — instead of being masked by the thread-affinity
``ProgrammingError``.

Mirrors the closed-first precedence already pinned for sync
``commit`` / ``rollback`` / ``cursor`` / ``row_factory.setter``.
"""

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
        ("text_factory", str),
    ],
)
def test_setter_on_closed_from_foreign_thread_raises_interface_error(name: str, value: Any) -> None:
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
        f"{name}.setter on a closed connection from a foreign thread "
        f"must raise InterfaceError (closed-state precedence per stdlib + "
        f"sibling setters' precedence discipline); got {type(captured[0]).__name__}: "
        f"{captured[0]}"
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
    """PEP 249 §6.4 + project discipline: every public Connection
    state-mutating method clears ``self.messages`` first."""
    c = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        c.messages.append((Exception, Exception("stale")))
        setattr(c, name, value)
        assert c.messages == []
    finally:
        c._closed_flag[0] = True
