"""Integration tests for INSERT/UPDATE/DELETE ... RETURNING support."""

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
class TestReturning:
    def test_insert_returning(self, cluster_address: str) -> None:
        """INSERT ... RETURNING should return rows via query path."""
        conn = connect(cluster_address, database="test_returning")
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE ret_test (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.execute("INSERT INTO ret_test (id, name) VALUES (1, 'alice') RETURNING id, name")
        row = cursor.fetchone()
        assert row == (1, "alice")

        cursor.execute("DROP TABLE ret_test")
        conn.close()

    def test_insert_returning_multiple(self, cluster_address: str) -> None:
        """INSERT ... RETURNING should support fetching all returned rows."""
        conn = connect(cluster_address, database="test_returning_multi")
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE ret_multi (id INTEGER PRIMARY KEY, val TEXT)")
        cursor.execute("INSERT INTO ret_multi (id, val) VALUES (1, 'a') RETURNING id, val")
        assert cursor.fetchone() == (1, "a")

        cursor.execute("INSERT INTO ret_multi (id, val) VALUES (2, 'b') RETURNING val")
        assert cursor.fetchone() == ("b",)

        cursor.execute("DROP TABLE ret_multi")
        conn.close()

    def test_delete_returning(self, cluster_address: str) -> None:
        """DELETE ... RETURNING should return deleted rows."""
        conn = connect(cluster_address, database="test_del_returning")
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE del_ret (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.execute("INSERT INTO del_ret (id, name) VALUES (1, 'alice')")
        cursor.execute("DELETE FROM del_ret WHERE id = 1 RETURNING id, name")
        row = cursor.fetchone()
        assert row == (1, "alice")

        cursor.execute("DROP TABLE del_ret")
        conn.close()

    def test_update_returning(self, cluster_address: str) -> None:
        """UPDATE ... RETURNING should return updated rows."""
        conn = connect(cluster_address, database="test_upd_returning")
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE upd_ret (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.execute("INSERT INTO upd_ret (id, name) VALUES (1, 'alice')")
        cursor.execute("UPDATE upd_ret SET name = 'bob' WHERE id = 1 RETURNING id, name")
        row = cursor.fetchone()
        assert row == (1, "bob")

        cursor.execute("DROP TABLE upd_ret")
        conn.close()
