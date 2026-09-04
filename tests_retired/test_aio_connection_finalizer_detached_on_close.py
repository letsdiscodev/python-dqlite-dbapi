"""AsyncConnection saves its weakref.finalize handle and detaches it on close."""

from __future__ import annotations

import weakref

from dqlitedbapi.aio import AsyncConnection


def test_async_connection_init_saves_finalizer_handle() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        assert aconn._finalizer is not None, (
            "AsyncConnection.__init__ must save the weakref.finalize handle"
        )
        assert isinstance(aconn._finalizer, weakref.finalize)
    finally:
        aconn.force_close_transport()


async def test_async_close_detaches_finalizer() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    assert aconn._finalizer is not None
    await aconn.close()
    assert aconn._finalizer is None, "close() must detach the finalizer; got non-None _finalizer"


def test_async_force_close_detaches_finalizer() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    assert aconn._finalizer is not None
    aconn.force_close_transport()
    assert aconn._finalizer is None, (
        "force_close_transport() must detach the finalizer; got non-None _finalizer"
    )


def test_double_close_is_idempotent_on_finalizer_detach() -> None:
    import asyncio

    aconn = AsyncConnection("127.0.0.1:9999")
    asyncio.run(aconn.close())
    assert aconn._finalizer is None
    asyncio.run(aconn.close())
    assert aconn._finalizer is None
