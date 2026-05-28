"""Pin: ``dqlitedbapi.Row`` is the cross-driver-portable equivalent
of stdlib ``sqlite3.Row`` — positional + column-name access,
``Mapping`` protocol, ``dict(row)`` / ``**row`` spread, without
the cursor-type constraint that makes stdlib's class unusable
on this driver.

Cross-driver porting: swap ``sqlite3.Row`` for ``dqlitedbapi.Row``
1:1; every documented stdlib idiom holds.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqlitedbapi import Row


def _cursor_with_description(*column_names: str) -> object:
    cur = MagicMock()
    cur.description = tuple((name, None, None, None, None, None, None) for name in column_names)
    return cur


def test_row_positional_access() -> None:
    cur = _cursor_with_description("x", "y")
    row = Row(cur, (1, 2))
    assert row[0] == 1
    assert row[1] == 2


def test_row_column_name_access() -> None:
    cur = _cursor_with_description("x", "y")
    row = Row(cur, (1, 2))
    assert row["x"] == 1
    assert row["y"] == 2


def test_row_unknown_column_raises_key_error() -> None:
    cur = _cursor_with_description("x")
    row = Row(cur, (1,))
    with pytest.raises(KeyError, match="missing"):
        _ = row["missing"]


def test_row_dict_conversion() -> None:
    cur = _cursor_with_description("x", "y")
    row = Row(cur, (1, 2))
    assert dict(row) == {"x": 1, "y": 2}


def test_row_keys_returns_column_names() -> None:
    cur = _cursor_with_description("x", "y")
    row = Row(cur, (1, 2))
    assert tuple(row.keys()) == ("x", "y")


def test_row_len_matches_value_count() -> None:
    cur = _cursor_with_description("x", "y", "z")
    row = Row(cur, (1, 2, 3))
    assert len(row) == 3


def test_row_equality() -> None:
    cur = _cursor_with_description("x", "y")
    row1 = Row(cur, (1, 2))
    row2 = Row(cur, (1, 2))
    assert row1 == row2


def test_row_double_spread_into_kwargs() -> None:
    """The ``**row`` spread requires the Mapping protocol to work
    on string keys.
    """
    cur = _cursor_with_description("a", "b")
    row = Row(cur, (10, 20))

    def consumer(*, a: int, b: int) -> int:
        return a + b

    assert consumer(**row) == 30


def test_row_rejects_non_int_non_str_key() -> None:
    cur = _cursor_with_description("x")
    row = Row(cur, (1,))
    with pytest.raises(TypeError, match="must be int or str"):
        _ = row[1.5]


def test_row_rejects_bool_key() -> None:
    # ``bool`` is an ``int`` subclass, so ``row[True]`` would otherwise
    # silently return column 1 and ``row[False]`` column 0. This driver
    # rejects ``bool``-where-an-int-is-expected everywhere else
    # (``arraysize`` setter, ``fetchmany`` size, ``scroll``,
    # ``_validate_ticks``) as a caller-bug trap; ``Row`` follows the
    # same discipline rather than silently coercing.
    cur = _cursor_with_description("x", "y")
    row = Row(cur, (10, 20))
    with pytest.raises(TypeError, match="must be int or str, not bool"):
        _ = row[True]
    with pytest.raises(TypeError, match="must be int or str, not bool"):
        _ = row[False]


def test_row_int_and_str_access_unaffected_by_bool_exclusion() -> None:
    # Regression guard for the ``bool`` exclusion: genuine ``int``
    # indexing (including negative) and column-name access keep working.
    cur = _cursor_with_description("x", "y")
    row = Row(cur, (10, 20))
    assert row[0] == 10
    assert row[1] == 20
    assert row[-1] == 20
    assert row["x"] == 10
    assert row["y"] == 20


def test_row_exported_at_top_level() -> None:
    assert hasattr(dqlitedbapi, "Row")
    assert "Row" in dqlitedbapi.__all__


def test_row_exported_from_aio_namespace() -> None:
    from dqlitedbapi import aio

    assert hasattr(aio, "Row")
    assert "Row" in aio.__all__
    assert aio.Row is Row


def test_row_repr() -> None:
    cur = _cursor_with_description("x", "y")
    row = Row(cur, (1, 2))
    assert "x=1" in repr(row)
    assert "y=2" in repr(row)
