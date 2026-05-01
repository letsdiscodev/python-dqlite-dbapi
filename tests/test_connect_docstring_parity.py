"""Pin: ``AsyncConnection.connect`` documents the same fail-fast /
optional contract as the sync ``Connection.connect``.

The sync sibling cross-references ``AsyncConnection.connect`` via
``Mirrors :meth:`AsyncConnection.connect``, so the async docstring
must carry the contract that mirror reference points at — otherwise
tooling that surfaces only docstrings (``help()``, IDE hover, Sphinx)
shows nothing actionable on the async side.
"""

from __future__ import annotations

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def test_async_connect_docstring_documents_fail_fast_contract() -> None:
    doc = AsyncConnection.connect.__doc__ or ""
    assert "fail-fast" in doc.lower(), (
        "AsyncConnection.connect must document the fail-fast / optional contract"
    )
    assert "optional" in doc.lower()


def test_async_connect_docstring_cross_references_sync_sibling() -> None:
    doc = AsyncConnection.connect.__doc__ or ""
    assert "Connection.connect" in doc, (
        "AsyncConnection.connect must cross-reference the sync sibling"
    )


def test_sync_connect_docstring_cross_references_async_sibling() -> None:
    """Pin the existing reverse pointer so a future refactor doesn't
    drop the sync docstring's mirror reference."""
    doc = Connection.connect.__doc__ or ""
    assert "AsyncConnection.connect" in doc
