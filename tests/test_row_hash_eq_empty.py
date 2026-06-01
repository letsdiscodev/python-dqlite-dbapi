"""``dqlitedbapi.Row`` hashability, ``__eq__`` vs a non-``Row``, and the
``description=None`` branch."""

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
    assert r.keys() == []
    assert dict(r) == {}
