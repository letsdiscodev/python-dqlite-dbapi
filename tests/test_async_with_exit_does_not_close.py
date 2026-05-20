"""Pin: ``AsyncConnection.__aexit__`` does NOT close on clean exit
— it performs commit / rollback only, matching stdlib
``sqlite3.Connection.__exit__``. Diverges from aiosqlite and
psycopg, both of which close on exit.

The asymmetry is deliberate (per the docstring on ``__aexit__``
and the matching note now on ``__aenter__``): cross-driver porters
from aiosqlite / psycopg must add an explicit
``await aconn.close()`` after the ``async with`` block. A future
PR that accidentally adds close-on-exit (a "consistency fix" or
copy-paste from a sibling driver) would trip this pin.

The sync sibling ``Connection.__exit__`` carries the same contract
and the same docstring; covered by the second test.
"""

from __future__ import annotations

import inspect

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def test_aexit_source_does_not_call_close_on_clean_exit() -> None:
    """PEP 343 stdlib parity: a clean exit commits but does NOT
    call ``self.close()`` / ``self._async_conn.close()``. Diverges
    from aiosqlite / psycopg which DO close on exit. A future PR
    that adds ``await self.close()`` to ``__aexit__`` (e.g. as a
    "consistency fix" copy-paste from a sibling driver) would
    trip this pin."""
    src = inspect.getsource(AsyncConnection.__aexit__)
    # Direct close call patterns to forbid:
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
    # The docstring must still acknowledge the divergence so
    # future maintainers see the rationale at the call-site.
    assert "aiosqlite" in src or "stdlib" in src, (
        "the divergence-from-aiosqlite / stdlib-parity note must "
        "remain in the method body or docstring as the contract anchor"
    )


def test_sync_exit_source_does_not_call_close_on_clean_exit() -> None:
    """Sync sibling :meth:`Connection.__exit__` must mirror the
    async contract — commit/rollback only, no close."""
    src = inspect.getsource(Connection.__exit__)
    assert "self.close()" not in src, (
        "sync parity broke: __exit__ now calls self.close(). "
        "Diverges from stdlib sqlite3.Connection.__exit__."
    )


def test_aenter_docstring_documents_asymmetric_lifecycle() -> None:
    """``__aenter__``'s docstring must explain the lazy-connect +
    no-close-on-exit asymmetry so cross-driver porters from
    aiosqlite / psycopg discover the footgun at the class API
    boundary, not only at the ``__aexit__`` site."""
    doc = AsyncConnection.__aenter__.__doc__ or ""
    assert "Asymmetric lifecycle" in doc
    assert "aiosqlite" in doc, (
        "docstring must name the divergent peer drivers so a grep "
        "for the porter's mental model surfaces the contract gap"
    )
    assert "aconn.close()" in doc, "docstring must point at the explicit close incantation"


def test_sync_enter_docstring_mirrors_async_asymmetric_lifecycle() -> None:
    """Sync sibling :meth:`Connection.__enter__` carries the same
    asymmetric-lifecycle docstring so the contract is symmetric
    across the sync and async surfaces."""
    doc = Connection.__enter__.__doc__ or ""
    assert "Asymmetric lifecycle" in doc
    assert "conn.close()" in doc
