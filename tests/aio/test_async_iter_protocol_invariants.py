"""Pin: ``AsyncCursor.__aiter__`` returns ``self`` even when closed
(PEP 492); the closed-state diagnostic is deferred to ``__anext__``."""

from __future__ import annotations

from dqlitedbapi.aio.connection import AsyncConnection


async def test_aiter_returns_self_invariance() -> None:
    """PEP 492 ``aiter(obj) is obj``: pin against returning a
    wrapper/generator."""
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    cur = aconn.cursor()
    assert cur.__aiter__() is cur
    assert aiter(cur) is cur


async def test_aiter_on_closed_cursor_does_not_raise() -> None:
    """``__aiter__`` on a closed cursor must NOT raise; the closed-state
    diagnostic is deferred to ``__anext__``."""
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    cur = aconn.cursor()
    cur.close()

    # Pre-fix this raised InterfaceError: _check_loop_binding ran a
    # closed-state check before returning self.
    same = cur.__aiter__()
    assert same is cur
