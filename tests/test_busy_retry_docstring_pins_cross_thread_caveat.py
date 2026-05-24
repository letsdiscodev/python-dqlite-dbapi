"""Pin: ``retry_sync_on_busy`` and ``retry_async_on_busy`` docstrings
document the cross-thread / cross-task interleaving semantic.

The retry loop releases ``_op_lock`` between attempts (the sleep
runs OUTSIDE the lock so it doesn't starve sibling threads). Under
``check_same_thread=False``, a BUSY-retried statement may observe
interleaved writes from sibling threads between retry attempts.

This caveat is documented but not runtime-enforced — the
documented contract is what users opt into when they share a
Connection across threads.
"""

from __future__ import annotations

from dqlitedbapi._busy_retry import retry_async_on_busy, retry_sync_on_busy


def test_sync_retry_docstring_documents_op_lock_release_between_attempts() -> None:
    doc = retry_sync_on_busy.__doc__ or ""
    assert "Cross-thread caveat" in doc, (
        "retry_sync_on_busy docstring must call out the cross-"
        "thread caveat header so users sharing a Connection find "
        "the contract"
    )
    assert "_op_lock" in doc, (
        "docstring must name the lock (`_op_lock`) so the implementation rationale is grepable"
    )


def test_sync_retry_docstring_documents_interleaving_example() -> None:
    doc = " ".join((retry_sync_on_busy.__doc__ or "").split())
    assert "check_same_thread" in doc
    assert "interleaved" in doc.lower() or "interleave" in doc.lower(), (
        "docstring must spell out the interleaving symptom: "
        "between retry attempts, sibling threads can land writes"
    )


def test_sync_retry_docstring_documents_transaction_workaround() -> None:
    doc = retry_sync_on_busy.__doc__ or ""
    assert "transaction" in doc.lower(), (
        "docstring must point at conn.transaction() as the atomic-snapshot workaround"
    )


def test_async_retry_docstring_documents_cross_task_caveat() -> None:
    doc = retry_async_on_busy.__doc__ or ""
    assert "Cross-task caveat" in doc, (
        "retry_async_on_busy docstring must call out the cross-"
        "task interleaving caveat (the asyncio analogue of the "
        "sync cross-thread caveat)"
    )


def test_async_retry_docstring_mentions_async_transaction_workaround() -> None:
    doc = retry_async_on_busy.__doc__ or ""
    assert "async with conn.transaction" in doc, (
        "async retry docstring must point at `async with "
        "conn.transaction():` as the atomic-snapshot workaround"
    )
