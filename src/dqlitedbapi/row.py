"""Stdlib ``sqlite3.Row``-equivalent row factory."""

from collections.abc import Iterator
from typing import Any, final

from dqlitedbapi.types import is_int_not_bool


@final
class Row:
    """Result row with positional and case-insensitive column-name access.

    Iteration yields values; ``row[i]`` and slices index positionally;
    ``row["name"]`` and ``keys()`` give name access. Unlike stdlib, a ``bool``
    index is rejected rather than coerced to int.
    """

    __slots__ = ("_columns", "_values")

    def __init__(self, cursor: object, row: tuple[Any, ...]) -> None:
        description = getattr(cursor, "description", None)
        self._columns: tuple[str, ...] = tuple(c[0] for c in description) if description else ()
        self._values: tuple[Any, ...] = tuple(row)

    def __getitem__(self, key: object) -> Any:
        if isinstance(key, slice):
            return self._values[key]
        if is_int_not_bool(key):
            return self._values[key]
        if isinstance(key, str):
            for idx, col in enumerate(self._columns):
                if col == key or (col.isascii() and key.isascii() and col.lower() == key.lower()):
                    return self._values[idx]
            raise KeyError(key)
        raise TypeError(f"Row indices must be int or str, not {type(key).__name__}")

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Row):
            return self._columns == other._columns and self._values == other._values
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self._columns, self._values))

    def keys(self) -> list[str]:
        return list(self._columns)

    def __repr__(self) -> str:
        pairs = ", ".join(f"{k}={v!r}" for k, v in zip(self._columns, self._values, strict=False))
        return f"Row({pairs})"
