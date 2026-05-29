"""Connection's docstring documents the check_same_thread=False contract anchors."""

from __future__ import annotations

import dqlitedbapi


def test_connection_docstring_documents_check_same_thread() -> None:
    doc = dqlitedbapi.Connection.__doc__ or ""
    assert "check_same_thread" in doc, (
        "Connection docstring must mention check_same_thread so "
        "users opting into the relaxation can find the contract"
    )


def test_connection_docstring_documents_per_thread_cursor_contract() -> None:
    # Whitespace-collapse so the phrase tolerates docstring re-wrapping.
    doc = " ".join((dqlitedbapi.Connection.__doc__ or "").split())
    assert "one cursor per thread" in doc, (
        "Connection docstring must spell out the per-thread cursor "
        "contract: cross-thread cursor sharing produces torn reads, "
        "not exceptions"
    )


def test_connection_docstring_documents_force_close_transport_workaround() -> None:
    doc = dqlitedbapi.Connection.__doc__ or ""
    assert "force_close_transport" in doc, (
        "Connection docstring must point at force_close_transport "
        "as the foreign-thread teardown workaround so operators "
        "reading the docs find SA's pool-recycle path"
    )


def test_connection_docstring_documents_messages_best_effort() -> None:
    doc = dqlitedbapi.Connection.__doc__ or ""
    assert "messages" in doc.lower() and "best-effort" in doc, (
        "Connection docstring must document that Connection.messages "
        "is best-effort under check_same_thread=False (PEP 249 §6.4 "
        "single-thread assumption broken)"
    )


def test_connection_docstring_documents_busy_retry_interleaving() -> None:
    doc = dqlitedbapi.Connection.__doc__ or ""
    assert "BUSY retries" in doc or "BUSY retry" in doc, (
        "Connection docstring must document that BUSY retries "
        "release the wire lock between attempts under "
        "check_same_thread=False (cross-link to "
        "retry_sync_on_busy)"
    )


def test_connection_docstring_documents_fork_check_unconditional() -> None:
    doc = dqlitedbapi.Connection.__doc__ or ""
    assert "fork" in doc.lower(), "must mention fork"
    assert "NEVER relaxed" in doc or "never relaxed" in doc.lower(), (
        "Connection docstring must spell out that the fork check "
        "is NEVER relaxed by check_same_thread (cross-process "
        "Connection use is always unsafe)"
    )
