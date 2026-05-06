"""Pin: ``AsyncConnection`` saves its ``weakref.finalize`` handle
and detaches it on orderly close so the finalizer queue does not
accumulate stale entries across closed instances.

Pre-fix, ``AsyncConnection.__init__`` discarded the
``weakref.finalize(...)`` return value — there was no
``self._finalizer = ...``, so ``close()`` and
``force_close_transport()`` could not call ``.detach()``. Result: the
registered finalizer plus its captured ``(closed_flag,
connected_flag, address)`` cells stayed on the ``weakref`` global
table for the lifetime of ``self`` even after orderly close —
inconsistent with the sync sibling (`connection.py:1399-1401,
1417-1419, 1581-1589`), ``DqliteConnection``, and ``ConnectionPool``,
all of which detach.

The fix saves the handle on ``self._finalizer`` and detaches in
``close()`` (regular + fork branch) and ``force_close_transport()``.
"""

from __future__ import annotations

import weakref

import pytest

from dqlitedbapi.aio import AsyncConnection


def test_async_connection_init_saves_finalizer_handle() -> None:
    """The handle returned by ``weakref.finalize(...)`` must be saved
    on ``self._finalizer`` so ``close()`` can ``detach()`` it later.
    """
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        assert aconn._finalizer is not None, (
            "AsyncConnection.__init__ must save the weakref.finalize handle"
        )
        assert isinstance(aconn._finalizer, weakref.finalize)
    finally:
        # Best-effort teardown without depending on a live cluster.
        aconn.force_close_transport()


@pytest.mark.asyncio
async def test_async_close_detaches_finalizer() -> None:
    """After ``close()``, the finalize handle must be detached so
    the ``weakref`` global table no longer carries the registration.
    """
    aconn = AsyncConnection("127.0.0.1:9999")
    # Don't actually connect — close() short-circuits via the early-
    # return on _closed; we manually mark the closed flag set from
    # __init__ AFTER asserting the finalizer exists.
    assert aconn._finalizer is not None
    await aconn.close()
    assert aconn._finalizer is None, "close() must detach the finalizer; got non-None _finalizer"


def test_async_force_close_detaches_finalizer() -> None:
    """``force_close_transport()`` is the sync teardown hook used by
    SA finalize / atexit / GC. It must also detach the finalizer
    (mirrors sync sibling discipline).
    """
    aconn = AsyncConnection("127.0.0.1:9999")
    assert aconn._finalizer is not None
    aconn.force_close_transport()
    assert aconn._finalizer is None, (
        "force_close_transport() must detach the finalizer; got non-None _finalizer"
    )


def test_double_close_is_idempotent_on_finalizer_detach() -> None:
    """A second ``close()`` after the first must not raise — the
    detach must be no-op-safe (we already nulled ``_finalizer``).
    """
    import asyncio

    aconn = AsyncConnection("127.0.0.1:9999")
    asyncio.run(aconn.close())
    assert aconn._finalizer is None
    # Second close — should be a no-op short-circuit.
    asyncio.run(aconn.close())
    assert aconn._finalizer is None
