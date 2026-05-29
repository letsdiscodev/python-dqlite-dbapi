"""Pin: ``AsyncConnection.connect`` documents the same fail-fast/optional contract
as the sync sibling, which cross-references it, so docstring-only tooling shows
something actionable on the async side too.
"""

from __future__ import annotations

from dqlitedbapi.aio.connection import AsyncConnection


def test_async_connect_docstring_documents_fail_fast_contract() -> None:
    doc = AsyncConnection.connect.__doc__ or ""
    assert "fail-fast" in doc.lower(), (
        "AsyncConnection.connect must document the fail-fast / optional contract"
    )
    assert "optional" in doc.lower()
