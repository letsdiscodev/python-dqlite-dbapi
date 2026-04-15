"""Integration tests for connection context manager transaction handling."""

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
class TestContextManagerTransactions:
    def test_context_manager_commits_on_clean_exit(self, cluster_address: str) -> None:
        """Connection context manager should commit on clean exit."""
        with connect(cluster_address, database="test_ctx_commit2") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS ctx_test")
            cursor.execute("CREATE TABLE ctx_test (id INTEGER PRIMARY KEY, val TEXT)")
            cursor.execute("BEGIN")
            cursor.execute("INSERT INTO ctx_test (id, val) VALUES (1, 'committed')")
            # __exit__ should commit

        # Verify data persisted
        with connect(cluster_address, database="test_ctx_commit2") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT val FROM ctx_test WHERE id = 1")
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "committed"
            cursor.execute("DROP TABLE ctx_test")

    def test_context_manager_rolls_back_on_exception(self, cluster_address: str) -> None:
        """Connection context manager should rollback on exception."""
        # Set up a table with known state
        with connect(cluster_address, database="test_ctx_rollback2") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS ctx_rb_test")
            cursor.execute("CREATE TABLE ctx_rb_test (id INTEGER PRIMARY KEY, val TEXT)")
            cursor.execute("INSERT INTO ctx_rb_test (id, val) VALUES (1, 'original')")

        # Update inside an explicit transaction, then raise
        with (
            pytest.raises(ValueError, match="simulated error"),
            connect(cluster_address, database="test_ctx_rollback2") as conn,
        ):
            cursor = conn.cursor()
            cursor.execute("BEGIN")
            cursor.execute("UPDATE ctx_rb_test SET val = 'changed' WHERE id = 1")
            raise ValueError("simulated error")
            # __exit__ should rollback

        # Verify original value is preserved (rolled back)
        with connect(cluster_address, database="test_ctx_rollback2") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT val FROM ctx_rb_test WHERE id = 1")
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "original"
            cursor.execute("DROP TABLE ctx_rb_test")
