"""Pin: result-row conversion skips the per-cell loop when no column
carries a registered converter, materialising rows directly as tuples.
"""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from dqlitedbapi import cursor as cursor_mod
from dqlitewire.constants import ValueType

_INT_ONLY_ROWS: list[list[Any]] = [[i, i + 1, "text"] for i in range(1000)]
_INT_ONLY_TYPES: list[list[int]] = [
    [int(ValueType.INTEGER), int(ValueType.INTEGER), int(ValueType.TEXT)] for _ in range(1000)
]
_INT_ONLY_COLUMN_TYPES: list[int] = [
    int(ValueType.INTEGER),
    int(ValueType.INTEGER),
    int(ValueType.TEXT),
]

_DT_ROWS: list[list[Any]] = [[1, "2024-01-01T00:00:00", 100] for _ in range(10)]
_DT_TYPES: list[list[int]] = [
    [int(ValueType.INTEGER), int(ValueType.ISO8601), int(ValueType.INTEGER)] for _ in range(10)
]
_DT_COLUMN_TYPES: list[int] = [
    int(ValueType.INTEGER),
    int(ValueType.ISO8601),
    int(ValueType.INTEGER),
]


def test_convert_rows_skips_per_cell_loop_when_no_converter_applies() -> None:
    """A result set with no convertible columns must not invoke ``_convert_row``."""
    with mock.patch.object(cursor_mod, "_convert_row", wraps=cursor_mod._convert_row) as spy:
        result = cursor_mod._convert_rows(_INT_ONLY_ROWS, _INT_ONLY_TYPES, _INT_ONLY_COLUMN_TYPES)

    assert spy.call_count == 0, (
        "_convert_rows must skip the per-cell loop when no column carries "
        f"a registered converter; got {spy.call_count} per-row calls"
    )
    assert len(result) == 1000
    assert result[0] == (0, 1, "text")
    assert all(isinstance(row, tuple) for row in result)


def test_convert_rows_runs_converter_when_column_type_matches() -> None:
    """A registered converter column (ISO8601) invokes ``_convert_row`` per row."""
    with mock.patch.object(cursor_mod, "_convert_row", wraps=cursor_mod._convert_row) as spy:
        result = cursor_mod._convert_rows(_DT_ROWS, _DT_TYPES, _DT_COLUMN_TYPES)

    assert spy.call_count == 10, (
        f"_convert_rows must invoke _convert_row per row when conversion is "
        f"needed; got {spy.call_count} calls (expected 10)"
    )
    assert result[0][1] != "2024-01-01T00:00:00", "ISO8601 column was not converted"


def test_convert_rows_runs_converter_when_row_types_diverge() -> None:
    """SQLite dynamic typing: per-row types can diverge from column_types,
    so the fast-path detector must inspect per-row types, not just column_types."""
    # column_types are all INTEGER, but the second row marks col 1 ISO8601.
    rows: list[list[Any]] = [[1, 2, 3], [4, "2024-06-15T12:00:00", 6]]
    row_types: list[list[int]] = [
        [int(ValueType.INTEGER), int(ValueType.INTEGER), int(ValueType.INTEGER)],
        [int(ValueType.INTEGER), int(ValueType.ISO8601), int(ValueType.INTEGER)],
    ]
    column_types: list[int] = [
        int(ValueType.INTEGER),
        int(ValueType.INTEGER),
        int(ValueType.INTEGER),
    ]

    with mock.patch.object(cursor_mod, "_convert_row", wraps=cursor_mod._convert_row) as spy:
        result = cursor_mod._convert_rows(rows, row_types, column_types)

    assert spy.call_count == 2, (
        f"_convert_rows must invoke _convert_row when ANY row carries a "
        f"convertible type; got {spy.call_count} calls (expected 2)"
    )
    assert result[1][1] != "2024-06-15T12:00:00", "ISO8601 cell from row 1 was not converted"


def test_convert_rows_empty_rows_returns_empty_list() -> None:
    """Zero rows must not raise."""
    result = cursor_mod._convert_rows([], [], [int(ValueType.INTEGER)])
    assert result == []


@pytest.mark.parametrize(
    "rows,row_types,column_types,expected",
    [
        # Plain integer cell with no converter.
        ([[1]], [[int(ValueType.INTEGER)]], [int(ValueType.INTEGER)], (1,)),
        # NULL cell — converter would skip even if registered.
        ([[None]], [[int(ValueType.ISO8601)]], [int(ValueType.ISO8601)], (None,)),
    ],
)
def test_convert_rows_basic_shapes(
    rows: list[list[Any]],
    row_types: list[list[int]],
    column_types: list[int],
    expected: tuple[Any, ...],
) -> None:
    result = cursor_mod._convert_rows(rows, row_types, column_types)
    assert result[0] == expected
