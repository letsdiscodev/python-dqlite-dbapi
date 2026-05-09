"""Pin: ``AsyncConnection.row_factory`` setter validates loop binding.

The sync sibling ``Connection.row_factory.setter`` at
``connection.py:1972-1985`` calls ``self._check_thread()`` to enforce
the "every state-mutating method enforces affinity" claim. The async
sibling at ``aio/connection.py:1358-1364`` did not check loop binding,
so a foreign-loop caller could silently mutate ``_row_factory`` and
affect cursors spawned on the legitimate loop.

The fix adds ``self._check_loop_binding()`` at the top of the async
setter (sibling of ``_ensure_locks`` that validates without binding
on first use; raises ``InterfaceError`` / ``ProgrammingError`` on
mismatch).
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import ProgrammingError


async def test_row_factory_setter_succeeds_on_bound_loop() -> None:
    """Negative pin: setting from the loop the connection is bound
    to is the documented happy path; the loop-binding check must
    not regress this.
    """
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        # Bind the loop by entering an async context that exercises
        # the lazy-bind path (``_ensure_locks`` is the canonical
        # binder; here we rely on the setter's ``_check_loop_binding``
        # which is no-op when not yet bound).
        aconn.row_factory = lambda cur, row: list(row)
        assert aconn._row_factory is not None
    finally:
        aconn.force_close_transport()


def test_row_factory_setter_raises_on_foreign_loop() -> None:
    """Pin: a foreign-loop caller setting ``row_factory`` raises
    ``ProgrammingError`` rather than silently mutating state shared
    with the legitimate loop.

    The repro: bind the connection on loop A, then attempt to set
    ``row_factory`` from inside a coroutine running on loop B.
    """
    aconn = AsyncConnection("127.0.0.1:9999")

    async def _bind_then_release() -> None:
        # _ensure_locks lazy-binds to the current loop.
        aconn._ensure_locks()

    # Run the binding on loop A.
    asyncio.run(_bind_then_release())

    # Now from a different loop in a different thread, attempt to
    # mutate row_factory.
    error_holder: list[BaseException] = []

    def _try_set_from_foreign_loop() -> None:
        async def _setter() -> None:
            try:
                aconn.row_factory = lambda cur, row: row
            except BaseException as e:
                error_holder.append(e)

        asyncio.run(_setter())

    t = threading.Thread(target=_try_set_from_foreign_loop)
    t.start()
    t.join(timeout=2.0)
    assert not t.is_alive()

    aconn.force_close_transport()

    assert error_holder, (
        "expected ProgrammingError from foreign-loop row_factory setter; got no exception"
    )
    assert isinstance(error_holder[0], ProgrammingError), (
        f"expected ProgrammingError, got {type(error_holder[0]).__name__}: {error_holder[0]}"
    )


def test_row_factory_setter_value_validation_still_runs() -> None:
    """Negative pin: the existing callable-or-None validation must
    still fire (non-callable, non-None values rejected). The loop-
    binding check runs FIRST but does not skip the validation on
    the bound-loop happy path.
    """
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        with pytest.raises(ProgrammingError, match="must be callable or None"):
            aconn.row_factory = "not callable"
    finally:
        aconn.force_close_transport()
