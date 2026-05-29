"""Pin: ``dqlitedbapi.Row`` Mapping-protocol surface that prior tests
did not cover — hashability, ``__eq__`` vs a non-``Row``, and
construction from a cursor with ``description=None``.

``Row`` is a ``@final`` documented stdlib-``sqlite3.Row`` replacement.
Existing tests cover positional/name indexing, ``dict(row)``,
``.keys()``, ``**row`` spread, and positive ``Row == Row`` equality —
but not ``hash(row)`` (a regression dropping ``__hash__`` would silently
make ``Row`` unusable as a dict key / set member), ``row == <non-Row>``
(must return ``False`` via ``NotImplemented``, not raise), or the
empty/``None``-description branch.
"""

from __future__ import annotations

from dqlitedbapi.row import Row


class _Cur:
    description = (("x", None, None, None, None, None, None),)


class _NoDescription:
    description = None


def test_row_is_hashable_and_usable_as_set_member() -> None:
    r1 = Row(_Cur(), (1,))
    r2 = Row(_Cur(), (1,))
    assert hash(r1) == hash(r2)
    assert r1 in {r2}
    assert {r1: "v"}[r2] == "v"


def test_row_eq_with_non_row_is_false_not_error() -> None:
    r = Row(_Cur(), (1,))
    assert (r == 5) is False
    assert (r == "x") is False
    assert (r != 5) is True


def test_row_with_none_description_has_no_columns() -> None:
    r = Row(_NoDescription(), ())
    assert len(r) == 0
    assert r.keys() == ()
    assert dict(r) == {}
