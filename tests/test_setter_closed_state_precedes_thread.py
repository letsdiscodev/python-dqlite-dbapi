"""Pin: ``Connection.autocommit.setter`` and
``Connection.isolation_level.setter`` order ``_check_thread()`` FIRST,
then ``_closed``, then ``messages`` clear. A cross-thread call on
any connection (closed or open) surfaces the thread-affinity
``ProgrammingError`` rather than the closed-state ``InterfaceError``
-- the project-wide reversal aligns the sync setters with the async
sibling's discipline so cross-thread misuse cannot mutate the owner
thread's ``messages`` list before the diagnostic fires.

The ``text_factory.setter`` raises ``NotSupportedError`` and is
unaffected by the precedence change (it surfaces before either
check).
"""

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
    """Thread-affinity precedence: foreign-thread setter even on a
    closed conn raises ProgrammingError (thread), not InterfaceError
    (closed). The thread check runs BEFORE the closed check so the
    cross-thread caller cannot reach the messages-clear step."""
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
    """PEP 249 §6.4 + project discipline: every public Connection
    state-mutating method clears ``self.messages`` first."""
    c = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        c.messages.append((Exception, Exception("stale")))
        setattr(c, name, value)
        assert c.messages == []
    finally:
        c._closed_flag[0] = True
