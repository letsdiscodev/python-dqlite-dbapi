"""Pin: retry helper docstrings document the cross-thread/cross-task interleaving caveat
(sleep runs outside ``_op_lock``, so sibling writes can land between BUSY retry attempts)."""

from __future__ import annotations

from dqlitedbapi._busy_retry import retry_sync_on_busy


def test_sync_retry_docstring_documents_transaction_workaround() -> None:
    doc = retry_sync_on_busy.__doc__ or ""
    assert "transaction" in doc.lower(), (
        "docstring must point at conn.transaction() as the atomic-snapshot workaround"
    )
