"""A stray commit()/rollback() inside ``with conn.transaction()`` raises InterfaceError.

The ctxmgr owns the boundaries; otherwise the stray commit ends the tx and the exit-time
rollback no-ops (in_transaction already False). Matches psycopg/psycopg2.
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
    """Bare commit() outside the ctxmgr is unaffected; the guard fires only for the owner."""
    conn = connect(cluster_address, database="test_sync_tx_stray_commit", timeout=2.0)
    try:
        conn.commit()  # no-op (no tx active) — must not raise
    finally:
        conn.close()


def test_nested_transaction_ctxmgr_rejected(cluster_address: str) -> None:
    """Nested ``with conn.transaction()`` raises: the inner commit would close the outer tx."""
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
    """A clean exit commits: the row inserted in the body persists."""
    conn = connect(cluster_address, database="test_sync_tx_commit_pin", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sync_tx_pin")
        cur.execute("CREATE TABLE sync_tx_pin (id INTEGER PRIMARY KEY, name TEXT)")

        with conn.transaction():
            cur.execute("INSERT INTO sync_tx_pin (id, name) VALUES (?, ?)", (1, "alice"))

        cur2 = conn.cursor()
        cur2.execute("SELECT id, name FROM sync_tx_pin WHERE id = ?", (1,))
        rows = cur2.fetchall()
        assert rows == [(1, "alice")]
    finally:
        conn.close()


def test_transaction_rollback_on_exception(cluster_address: str) -> None:
    """A body exception triggers ROLLBACK; the inserted row must not persist."""
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
