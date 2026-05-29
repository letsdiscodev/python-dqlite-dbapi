"""Stdlib ``sqlite3.Row``-equivalent row factory for dqlite.

Cross-driver porting target: stdlib ``sqlite3.Row``'s C-extension
constructor type-checks ``argument 1`` to be a ``pysqlite_CursorType``,
so wiring ``conn.row_factory = sqlite3.Row`` on this driver crashes
at the first fetch with ``DataError("row_factory call failed:
argument 1 must be sqlite3.Cursor, not Cursor")``. ``dqlitedbapi.Row``
is the same surface (positional + column-name indexing, ``.keys()``,
``dict(row)`` conversion) without the cursor-type constraint.

Use the same idiom on dqlite as on stdlib::

    conn.row_factory = dqlitedbapi.Row
    cur = conn.execute("SELECT 1 AS x, 2 AS y")
    row = cur.fetchone()
    assert row["x"] == 1
    assert row[0] == 1
    assert tuple(row) == (1, 2)
    assert list(row.keys()) == ["x", "y"]
    assert dict(row) == {"x": 1, "y": 2}
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, final


@final
class Row:
    """Stdlib ``sqlite3.Row``-equivalent row factory.

    Wraps a result tuple with column-name and positional access. Like
    stdlib ``sqlite3.Row`` it is a SEQUENCE, not a mapping: iteration
    (``tuple(row)`` / ``list(row)`` / ``for v in row`` / ``a, b = row``)
    yields VALUES, and ``row[i]`` / ``row[i:j]`` index positionally.
    ``row["name"]`` and ``.keys()`` provide column-name access, and
    ``dict(row)`` / ``**row`` work via ``keys()`` + ``__getitem__``.

    Unlike stdlib ``sqlite3.Row``, this class accepts any cursor
    object that exposes a ``description`` attribute (the canonical
    PEP 249 surface) — it does NOT type-check the cursor argument.
    Cross-driver porting from stdlib ``sqlite3`` should swap
    ``sqlite3.Row`` for ``dqlitedbapi.Row`` 1:1. The one deliberate
    divergence: a ``bool`` index (``row[True]``) is rejected with
    ``TypeError`` rather than silently coerced to ``row[1]`` the way
    stdlib does — a ``bool`` index is almost always a caller bug.
    """

    __slots__ = ("_columns", "_values")

    def __init__(self, cursor: object, row: tuple[Any, ...]) -> None:
        # ``description`` follows PEP 249 §6.1.2: a sequence of
        # 7-tuples whose first element is the column name. Tolerate
        # ``description=None`` (no-result-set fetch — defensive).
        description = getattr(cursor, "description", None)
        if description:
            self._columns: tuple[str, ...] = tuple(c[0] for c in description)
        else:
            self._columns = ()
        self._values: tuple[Any, ...] = tuple(row)

    def __getitem__(self, key: object) -> Any:
        # Slice indexing returns a tuple of values, matching stdlib
        # ``sqlite3.Row`` (``row[0:2]`` -> ``(v0, v1)``).
        if isinstance(key, slice):
            return self._values[key]
        # ``bool`` is an ``int`` subclass: without the explicit
        # exclusion ``row[True]`` would silently return column 1 and
        # ``row[False]`` column 0. A ``bool`` index is almost always a
        # caller bug, so reject it rather than coerce — matching the
        # ``bool``-where-int-expected traps used elsewhere in this
        # driver (``arraysize`` setter, ``fetchmany`` size, ``scroll``,
        # ``_validate_ticks``).
        if isinstance(key, int) and not isinstance(key, bool):
            return self._values[key]
        if isinstance(key, str):
            try:
                idx = self._columns.index(key)
            except ValueError as exc:
                raise KeyError(key) from exc
            return self._values[idx]
        raise TypeError(f"Row indices must be int or str, not {type(key).__name__}")

    def __iter__(self) -> Iterator[Any]:
        # Sequence iteration yields VALUES, matching stdlib
        # ``sqlite3.Row`` (``tuple(row)`` / ``list(row)`` / ``a, b =
        # row``). ``dict(row)`` and ``**row`` do NOT use this — CPython
        # consumes ``keys()`` + ``__getitem__`` when ``keys()`` exists.
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
        """Return the column names tuple — matches stdlib
        ``sqlite3.Row.keys()`` shape (list-like). Used by ``dict(row)``
        and ``**row`` for name-keyed access.
        """
        return self._columns

    def __repr__(self) -> str:
        pairs = ", ".join(f"{k}={v!r}" for k, v in zip(self._columns, self._values, strict=False))
        return f"Row({pairs})"
