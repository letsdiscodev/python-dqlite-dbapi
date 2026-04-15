"""Integration tests for query detection: CTEs, comments, etc."""

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
class TestQueryDetection:
    def test_cte_select(self, cluster_address: str) -> None:
        """WITH ... SELECT (CTE) should return rows via query path."""
        with connect(cluster_address, database="test_cte_select") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS cte_test")
            cursor.execute("CREATE TABLE cte_test (id INTEGER PRIMARY KEY, name TEXT)")
            cursor.execute("INSERT INTO cte_test (id, name) VALUES (1, 'alice')")

            cursor.execute("WITH names AS (SELECT id, name FROM cte_test) SELECT * FROM names")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0] == (1, "alice")

            cursor.execute("DROP TABLE cte_test")

    def test_comment_before_select(self, cluster_address: str) -> None:
        """-- comment before SELECT should still return rows."""
        with connect(cluster_address, database="test_comment_select") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS comment_test")
            cursor.execute("CREATE TABLE comment_test (id INTEGER PRIMARY KEY)")
            cursor.execute("INSERT INTO comment_test (id) VALUES (1)")

            cursor.execute("-- this is a comment\nSELECT * FROM comment_test")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0] == (1,)

            cursor.execute("DROP TABLE comment_test")

    def test_block_comment_before_select(self, cluster_address: str) -> None:
        """/* block comment */ before SELECT should still return rows."""
        with connect(cluster_address, database="test_block_comment_select") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS bcomment_test")
            cursor.execute("CREATE TABLE bcomment_test (id INTEGER PRIMARY KEY)")
            cursor.execute("INSERT INTO bcomment_test (id) VALUES (42)")

            cursor.execute("/* request_id=abc123 */ SELECT * FROM bcomment_test")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0] == (42,)

            cursor.execute("DROP TABLE bcomment_test")

    def test_recursive_cte(self, cluster_address: str) -> None:
        """WITH RECURSIVE ... SELECT should return rows."""
        with connect(cluster_address, database="test_recursive_cte_select") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "WITH RECURSIVE cnt(x) AS "
                "(VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<5) "
                "SELECT x FROM cnt"
            )
            rows = cursor.fetchall()
            assert len(rows) == 5
            assert rows == [(1,), (2,), (3,), (4,), (5,)]
