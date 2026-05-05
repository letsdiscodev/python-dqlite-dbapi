"""Pin: ``paramstyle = "qmark"`` only — non-qmark placeholders fall
through to bind-count rejection.

The driver advertises ``paramstyle = "qmark"``. PEP 249 callers porting
from psycopg (``%s`` / ``%(name)s``) or asyncpg (``$1`` / ``$2``)
should not silently round-trip a bad query to the cluster.

Today's behaviour:

- ``cur.execute(":name", {"name": x})`` is rejected up front by
  ``_reject_non_sequence_params`` (mappings are not accepted).
- ``cur.execute(":name", [x])`` falls through to the bind-count
  check: SQL has 0 ``?`` placeholders, params has 1 → raises
  ``ProgrammingError("Incorrect number of bindings")`` before any
  wire round-trip.
- ``cur.execute("%s", (x,))`` is the same shape: 0 ``?`` placeholders
  vs 1 param.

A future refactor that loosens placeholder counting (e.g., starts
counting `:name` placeholders) would silently slip these patterns
through to a server-side `SQLITE_RANGE` error. Pin the current
behaviour.
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
    """Mappings are rejected up front — a clean ``ProgrammingError``
    at the call site rather than a wire round-trip."""
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT :name", {"name": "x"})  # type: ignore[arg-type]


def test_named_param_sql_with_list_falls_through_to_bind_count(
    cur: dqlitedbapi.Cursor,
) -> None:
    """``:name`` SQL with a sequence of length 1 has placeholder
    count=0, param count=1 → bind-count rejection. Pin the current
    behaviour."""
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
        cur.execute("SELECT :name", ["x"])


def test_pyformat_sql_with_tuple_falls_through_to_bind_count(
    cur: dqlitedbapi.Cursor,
) -> None:
    """``%s`` (psycopg-style) SQL with a tuple of length 1 falls
    through to bind-count rejection. Pin the current behaviour."""
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
        cur.execute("SELECT %s", ("x",))


def test_qmark_sql_works(cur: dqlitedbapi.Cursor) -> None:
    """Sanity: the canonical qmark form executes without a wire RTT
    error (qmark is the advertised style)."""
    cur.execute("SELECT ?", ("x",))
    assert cur.fetchone() == ("x",)
