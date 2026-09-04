"""commit()/rollback() on an unused connection must be a no-op, not a new TCP dial."""

import dqlitedbapi

# Nothing listens on port 1: any dial attempt would raise OperationalError.
UNREACHABLE = "127.0.0.1:1"


def test_commit_on_unused_connection_is_noop() -> None:
    conn = dqlitedbapi.connect(UNREACHABLE, timeout=2.0)
    conn.commit()
    conn.close()


def test_rollback_on_unused_connection_is_noop() -> None:
    conn = dqlitedbapi.connect(UNREACHABLE, timeout=2.0)
    conn.rollback()
    conn.close()
