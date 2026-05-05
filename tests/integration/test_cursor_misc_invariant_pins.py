"""Pins for documented but previously-unverified Cursor invariants:

- description tuple-identity invariance across consecutive accesses
- fetchall idempotency on an exhausted cursor (twice → ``[]``)
- closed cursor's ``.connection`` still resolves while parent
  Connection is alive (weakref.proxy round-trip)
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

import dqlitedbapi


@pytest.fixture
def cur_with_rows() -> Iterator[dqlitedbapi.Cursor]:
    """A cursor primed with three integer rows from a temp table.
    Uses the live cluster (integration-style) so the description
    tuple comes from a real wire response."""
    conn = dqlitedbapi.connect("localhost:9001", database="test_misc_pins")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS desc_pin")
    cur.execute("CREATE TABLE desc_pin (id INTEGER, name TEXT)")
    cur.execute("INSERT INTO desc_pin VALUES (1,'a'), (2,'b'), (3,'c')")
    conn.commit()
    cur.execute("SELECT id, name FROM desc_pin ORDER BY id")
    yield cur
    conn.close()


@pytest.mark.integration
def test_description_returns_same_tuple_object_across_accesses(
    cur_with_rows: dqlitedbapi.Cursor,
) -> None:
    """stdlib sqlite3 parity: ``cur.description`` returns the same
    tuple object on each access (a tuple is structurally immutable;
    no defensive copy needed). A future refactor that built the
    tuple lazily would silently break callers comparing
    ``cur.description is prior_desc`` for change detection."""
    d1 = cur_with_rows.description
    d2 = cur_with_rows.description
    assert d1 is d2


@pytest.mark.integration
def test_fetchall_twice_returns_empty_second_time(
    cur_with_rows: dqlitedbapi.Cursor,
) -> None:
    first = cur_with_rows.fetchall()
    assert len(first) == 3
    second = cur_with_rows.fetchall()
    assert second == []
    third = cur_with_rows.fetchall()  # idempotent terminal state
    assert third == []


@pytest.mark.integration
def test_fetchall_then_fetchone_returns_none(cur_with_rows: dqlitedbapi.Cursor) -> None:
    cur_with_rows.fetchall()
    assert cur_with_rows.fetchone() is None


@pytest.mark.integration
def test_fetchall_then_fetchmany_returns_empty(cur_with_rows: dqlitedbapi.Cursor) -> None:
    cur_with_rows.fetchall()
    assert cur_with_rows.fetchmany(10) == []


@pytest.mark.integration
def test_closed_cursor_dot_connection_still_resolves_while_conn_alive() -> None:
    """The ``connection`` property must keep working on a closed cursor
    as long as the parent Connection is alive (weakref.proxy round-
    trip). A future refactor that raised InterfaceError on every
    closed cursor's .connection access would silently break SA's
    ``cursor.connection`` introspection (which fires on every checked-
    out cursor)."""
    conn = dqlitedbapi.connect("localhost:9001", database="test_misc_pins")
    try:
        cur = conn.cursor()
        cur.close()
        # _connection is now a weakref.proxy; the property must
        # still resolve transparently.
        resolved = cur.connection
        # weakref.proxy != target via ``is``; compare via address.
        assert resolved.address == conn.address
    finally:
        conn.close()
