"""Pin the lastrowid-vs-RETURNING divergence from stdlib sqlite3.

dqlite's wire protocol returns ``last_insert_id`` only on Exec responses, not
row-returning ones, so ``cursor.lastrowid`` is not updated by INSERT ... RETURNING.
"""

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
class TestLastrowidWithReturning:
    def test_lastrowid_not_updated_by_insert_returning(self, cluster_address: str) -> None:
        with connect(cluster_address, database="test_lastrowid_returning") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS lri_test")
            cursor.execute("CREATE TABLE lri_test (id INTEGER PRIMARY KEY AUTOINCREMENT, v INT)")
            # DDL surfaces last_insert_id=0 on its Exec response.
            lastrowid_after_ddl = cursor.lastrowid
            cursor.execute("INSERT INTO lri_test (v) VALUES (?) RETURNING id", (42,))
            returned_id = cursor.fetchone()[0]
            assert returned_id >= 1
            # RETURNING does not update lastrowid; it still holds the Exec-path value.
            assert cursor.lastrowid == lastrowid_after_ddl
            assert cursor.lastrowid != returned_id
            cursor.execute("DROP TABLE lri_test")

    def test_lastrowid_unchanged_after_returning_following_non_returning(
        self, cluster_address: str
    ) -> None:
        with connect(cluster_address, database="test_lastrowid_returning_2") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS lri_test2")
            cursor.execute("CREATE TABLE lri_test2 (id INTEGER PRIMARY KEY AUTOINCREMENT, v INT)")
            cursor.execute("INSERT INTO lri_test2 (v) VALUES (?)", (1,))
            first_rowid = cursor.lastrowid
            assert first_rowid is not None and first_rowid >= 1

            cursor.execute("INSERT INTO lri_test2 (v) VALUES (?) RETURNING id", (2,))
            cursor.fetchone()
            # lastrowid from the Exec-path INSERT is preserved; RETURNING does not update it.
            assert cursor.lastrowid == first_rowid
            cursor.execute("DROP TABLE lri_test2")
