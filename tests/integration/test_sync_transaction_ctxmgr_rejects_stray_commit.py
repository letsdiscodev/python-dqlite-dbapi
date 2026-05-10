"""Pin: ``with conn.transaction(): conn.commit()`` raises
``InterfaceError`` instead of silently exiting the transaction.

Sync sibling of ``test_async_transaction_ctxmgr_rejects_stray_commit``.
The ctxmgr owns transaction boundaries — a stray ``conn.commit()``
inside the body would otherwise end the transaction without exiting
the ``with`` block, and the surrounding rollback-at-exit no-ops
because ``in_transaction`` is already False. psycopg /
psycopg2.connection both reject the same shape.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi import connect


def test_commit_inside_transaction_ctxmgr_raises(cluster_address: str) -> None:
    conn = connect(cluster_address, database="test_sync_tx_stray_commit", timeout=2.0)
    try:
        with (
            conn.transaction(),
            pytest.raises(dqlitedbapi.InterfaceError, match="context manager"),
        ):
            conn.commit()
    finally:
        conn.close()


def test_rollback_inside_transaction_ctxmgr_raises(cluster_address: str) -> None:
    conn = connect(cluster_address, database="test_sync_tx_stray_commit", timeout=2.0)
    try:
        with (
            conn.transaction(),
            pytest.raises(dqlitedbapi.InterfaceError, match="context manager"),
        ):
            conn.rollback()
    finally:
        conn.close()


def test_commit_outside_transaction_ctxmgr_still_works(
    cluster_address: str,
) -> None:
    """Negative pin: bare ``conn.commit()`` outside the ctxmgr is
    unaffected. The ctxmgr-owns-boundaries guard fires only when the
    owner-thread token matches."""
    conn = connect(cluster_address, database="test_sync_tx_stray_commit", timeout=2.0)
    try:
        conn.commit()  # no-op (autocommit; no tx active) — must not raise
    finally:
        conn.close()


def test_nested_transaction_ctxmgr_rejected(cluster_address: str) -> None:
    """Pin: nested ``with conn.transaction()`` raises immediately.
    Two levels of ctxmgr would have ambiguous semantics — the inner
    body's commit would close the outer's transaction. Match the
    async sibling's nested-rejection discipline."""
    conn = connect(cluster_address, database="test_sync_tx_stray_commit", timeout=2.0)
    try:
        with (
            conn.transaction(),
            pytest.raises(dqlitedbapi.InterfaceError, match="Nested"),
            conn.transaction(),
        ):
            pass
    finally:
        conn.close()


def test_transaction_commits_on_clean_exit(cluster_address: str) -> None:
    """Happy-path pin: a clean exit commits. Inserts a row inside the
    body, exits cleanly, then re-reads to confirm the row persists."""
    conn = connect(cluster_address, database="test_sync_tx_commit_pin", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sync_tx_pin")
        cur.execute("CREATE TABLE sync_tx_pin (id INTEGER PRIMARY KEY, name TEXT)")

        with conn.transaction():
            cur.execute("INSERT INTO sync_tx_pin (id, name) VALUES (?, ?)", (1, "alice"))

        # New cursor (defensive — the original would also work).
        cur2 = conn.cursor()
        cur2.execute("SELECT id, name FROM sync_tx_pin WHERE id = ?", (1,))
        rows = cur2.fetchall()
        assert rows == [(1, "alice")]
    finally:
        conn.close()


def test_transaction_rollback_on_exception(cluster_address: str) -> None:
    """A body exception triggers ROLLBACK — the inserted row must NOT
    persist after the ``with`` block re-raises."""
    conn = connect(cluster_address, database="test_sync_tx_rollback_pin", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sync_tx_rb_pin")
        cur.execute("CREATE TABLE sync_tx_rb_pin (id INTEGER PRIMARY KEY, name TEXT)")

        with pytest.raises(RuntimeError, match="body raised"), conn.transaction():
            cur.execute("INSERT INTO sync_tx_rb_pin (id, name) VALUES (?, ?)", (1, "alice"))
            raise RuntimeError("body raised")

        cur2 = conn.cursor()
        cur2.execute("SELECT COUNT(*) FROM sync_tx_rb_pin")
        rows = cur2.fetchall()
        assert rows == [(0,)], "row from rolled-back tx must not persist"
    finally:
        conn.close()


def test_transaction_on_closed_connection_raises(cluster_address: str) -> None:
    conn = connect(cluster_address, database="test_sync_tx_closed_pin", timeout=2.0)
    conn.close()
    with pytest.raises(dqlitedbapi.InterfaceError, match="closed"), conn.transaction():
        pass
