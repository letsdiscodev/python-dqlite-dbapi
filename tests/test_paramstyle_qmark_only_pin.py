"""Pin: ``paramstyle = "qmark"`` only.

Non-qmark placeholders (``:name``, ``%s``, ``$1``) hit mapping rejection or the
?-count check before any wire round-trip, never a server-side SQLITE_RANGE error.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

import dqlitedbapi
from dqlitedbapi import ProgrammingError


@pytest.fixture
def cur() -> Iterator[dqlitedbapi.Cursor]:
    conn = dqlitedbapi.connect("localhost:9001", timeout=2.0)
    cursor = conn.cursor()
    yield cursor
    conn.close()


def test_named_param_sql_with_dict_rejected_with_mapping_diagnostic(
    cur: dqlitedbapi.Cursor,
) -> None:
    """Mappings are rejected up front, before any wire round-trip."""
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT :name", {"name": "x"})  # type: ignore[arg-type]


def test_named_param_sql_with_list_falls_through_to_bind_count(
    cur: dqlitedbapi.Cursor,
) -> None:
    """``:name`` SQL (0 placeholders) with a 1-element sequence hits bind-count."""
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
        cur.execute("SELECT :name", ["x"])


def test_pyformat_sql_with_tuple_falls_through_to_bind_count(
    cur: dqlitedbapi.Cursor,
) -> None:
    """``%s`` (psycopg-style) SQL with a 1-tuple hits bind-count rejection."""
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
        cur.execute("SELECT %s", ("x",))


def test_qmark_sql_works(cur: dqlitedbapi.Cursor) -> None:
    """The canonical qmark form executes cleanly (the advertised style)."""
    cur.execute("SELECT ?", ("x",))
    assert cur.fetchone() == ("x",)
