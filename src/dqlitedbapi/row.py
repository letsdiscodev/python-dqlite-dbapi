"""Stdlib ``sqlite3.Row``-equivalent row factory for dqlite.

Same surface as ``sqlite3.Row`` but without its C-extension cursor type-check, which crashes
when ``conn.row_factory = sqlite3.Row`` is set on a non-stdlib cursor.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, final

from dqlitedbapi._constants import _is_int_not_bool


@final
class Row:
    """Sequence (not mapping) wrapping a result tuple with positional and column-name access.

    Iteration yields values; ``row[i]``/slices index positionally; ``row["name"]``/``.keys()``
    give name access. Accepts any object with a ``description`` attribute. Deliberate divergence
    from stdlib: a ``bool`` index is rejected rather than coerced to int.
    """

    __slots__ = ("_columns", "_values")

    def __init__(self, cursor: object, row: tuple[Any, ...]) -> None:
        # Tolerate ``description=None`` (no-result-set fetch).
        description = getattr(cursor, "description", None)
        if description:
            self._columns: tuple[str, ...] = tuple(c[0] for c in description)
        else:
            self._columns = ()
        self._values: tuple[Any, ...] = tuple(row)

    def __getitem__(self, key: object) -> Any:
        if isinstance(key, slice):
            return self._values[key]
        # ``bool`` is an ``int`` subclass; reject it rather than let ``row[True]`` mean column 1.
        if _is_int_not_bool(key):
            return self._values[key]
        if isinstance(key, str):
            try:
                idx = self._columns.index(key)
            except ValueError as exc:
                raise KeyError(key) from exc
            return self._values[idx]
        raise TypeError(f"Row indices must be int or str, not {type(key).__name__}")

    def __iter__(self) -> Iterator[Any]:
        # Yields values, not keys; ``dict(row)``/``**row`` use ``keys()`` + ``__getitem__``.
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Row):
            return self._columns == other._columns and self._values == other._values
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self._columns, self._values))

    def keys(self) -> tuple[str, ...]:
        return self._columns

    def __repr__(self) -> str:
        pairs = ", ".join(f"{k}={v!r}" for k, v in zip(self._columns, self._values, strict=False))
        return f"Row({pairs})"
