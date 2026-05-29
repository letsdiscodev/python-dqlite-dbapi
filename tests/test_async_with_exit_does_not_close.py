"""Pin: ``__aexit__`` commits/rolls back but does NOT close (stdlib parity);
diverges from aiosqlite/psycopg which close on exit."""

from __future__ import annotations

import inspect

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def test_aexit_source_does_not_call_close_on_clean_exit() -> None:
    src = inspect.getsource(AsyncConnection.__aexit__)
    assert "await self.close()" not in src, (
        "stdlib parity broke: __aexit__ now awaits self.close(). "
        "Diverges from sqlite3.Connection.__exit__; matches aiosqlite "
        "/ psycopg. Re-evaluate the contract documented in the "
        "method's note block and in :meth:`__aenter__` before "
        "accepting this change."
    )
    assert "self._async_conn.close()" not in src, (
        "stdlib parity broke: __aexit__ now directly closes the "
        "inner connection. See the note block."
    )
    assert "aiosqlite" in src or "stdlib" in src, (
        "the divergence-from-aiosqlite / stdlib-parity note must "
        "remain in the method body or docstring as the contract anchor"
    )


def test_sync_exit_source_does_not_call_close_on_clean_exit() -> None:
    src = inspect.getsource(Connection.__exit__)
    assert "self.close()" not in src, (
        "sync parity broke: __exit__ now calls self.close(). "
        "Diverges from stdlib sqlite3.Connection.__exit__."
    )


def test_aenter_docstring_documents_asymmetric_lifecycle() -> None:
    doc = AsyncConnection.__aenter__.__doc__ or ""
    assert "Asymmetric lifecycle" in doc
    assert "aiosqlite" in doc, (
        "docstring must name the divergent peer drivers so a grep "
        "for the porter's mental model surfaces the contract gap"
    )
    assert "aconn.close()" in doc, "docstring must point at the explicit close incantation"
