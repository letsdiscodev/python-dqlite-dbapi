"""Pin the lastrowid-vs-RETURNING divergence from stdlib sqlite3.

dqlite's wire protocol does not return ``last_insert_id`` on
row-returning responses (only on Exec responses). After
``INSERT ... RETURNING`` the dbapi cursor.lastrowid stays ``None``
(or the prior value from a non-RETURNING INSERT). Callers must read
the id from the returned row instead.

This is documented divergence from ``sqlite3.Cursor.lastrowid``
which does update after RETURNING. Pinning the current behavior so
a future protocol-level change surfaces as a regression that can be
triaged deliberately.
"""

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
class TestLastrowidWithReturning:
    def test_lastrowid_not_updated_by_insert_returning(self, cluster_address: str) -> None:
        """After ``INSERT ... RETURNING``, cursor.lastrowid reflects
        the prior value from the DDL/Exec path (not the inserted id).
        The rowid must be read from the returned row.
        """
        with connect(cluster_address, database="test_lastrowid_returning") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS lri_test")
            cursor.execute("CREATE TABLE lri_test (id INTEGER PRIMARY KEY AUTOINCREMENT, v INT)")
            # DDL surfaces last_insert_id=0 on its Exec response.
            lastrowid_after_ddl = cursor.lastrowid
            cursor.execute("INSERT INTO lri_test (v) VALUES (?) RETURNING id", (42,))
            returned_id = cursor.fetchone()[0]  # type: ignore[index]
            # The only authoritative source of the rowid is the returned row.
            assert returned_id >= 1
            # lastrowid is NOT updated by the RETURNING path — it still
            # holds whatever the Exec path last wrote. Explicitly NOT
            # equal to the newly inserted id.
            assert cursor.lastrowid == lastrowid_after_ddl
            assert cursor.lastrowid != returned_id
            cursor.execute("DROP TABLE lri_test")

    def test_lastrowid_unchanged_after_returning_following_non_returning(
        self, cluster_address: str
    ) -> None:
        """After a non-RETURNING INSERT sets lastrowid, a subsequent
        RETURNING INSERT must not overwrite it (since RETURNING does not
        surface last_insert_id). Validates that the divergence direction
        is consistent: RETURNING never touches lastrowid.
        """
        with connect(cluster_address, database="test_lastrowid_returning_2") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS lri_test2")
            cursor.execute("CREATE TABLE lri_test2 (id INTEGER PRIMARY KEY AUTOINCREMENT, v INT)")
            cursor.execute("INSERT INTO lri_test2 (v) VALUES (?)", (1,))
            first_rowid = cursor.lastrowid
            assert first_rowid is not None and first_rowid >= 1

            cursor.execute("INSERT INTO lri_test2 (v) VALUES (?) RETURNING id", (2,))
            cursor.fetchone()
            # lastrowid from the Exec-path INSERT is preserved; the
            # RETURNING path does not update it.
            assert cursor.lastrowid == first_rowid
            cursor.execute("DROP TABLE lri_test2")
