"""``_ExecuteManyAccumulator.push`` decouples rows-returned vs rows-affected.

Prior implementation summed ``cursor._rowcount`` unconditionally, which
relied on the execute path's overload (``_rowcount = len(rows)`` on the
RETURNING branch) to produce the correct ``total_affected``. A future
change that makes ``_rowcount`` actually mean "rows affected" (distinct
from ``len(rows)`` for e.g. ``INSERT ... ON CONFLICT ... RETURNING``
where rowcount may include skipped rows) would silently double-count.

The new push() branches on ``_description is not None``:
- Row-returning: ``total_affected += len(rows)``.
- Plain DML: ``total_affected += _rowcount``.

Both paths are now explicit; neither depends on the overload.
"""

from __future__ import annotations

from dqlitedbapi.cursor import _ExecuteManyAccumulator


class _FakeCursor:
    def __init__(
        self,
        *,
        rowcount: int = 0,
        description: object = None,
        rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self._rowcount = rowcount
        self._description = description
        self._rows = rows or []
        self._row_index = 0


class TestPushPlainDML:
    def test_sums_rowcount_over_iterations(self) -> None:
        acc = _ExecuteManyAccumulator()
        for rc in [1, 2, 3]:
            acc.push(_FakeCursor(rowcount=rc))  # type: ignore[arg-type]
        assert acc.total_affected == 6
        assert acc.rows == []
        assert acc.description is None

    def test_negative_rowcount_ignored(self) -> None:
        """Guard: PEP 249 allows ``rowcount = -1`` ("not determinable")
        — don't let a stray -1 decrement the running total."""
        acc = _ExecuteManyAccumulator()
        acc.push(_FakeCursor(rowcount=2))  # type: ignore[arg-type]
        acc.push(_FakeCursor(rowcount=-1))  # type: ignore[arg-type]
        assert acc.total_affected == 2


class TestPushRowReturning:
    def test_total_affected_uses_len_rows_not_rowcount(self) -> None:
        """The invariant: on the RETURNING branch, total_affected sums
        ``len(rows)`` — not ``_rowcount`` — so a future rowcount
        overload change can't silently double-count.
        """
        acc = _ExecuteManyAccumulator()
        desc = (("id", 1, None, None, None, None, None),)
        # Cursor deliberately lies: ``_rowcount=99`` but only 2 rows.
        # Push must use len(rows)=2, not 99, to keep the invariant
        # stable against a future rowcount-semantics change.
        acc.push(
            _FakeCursor(  # type: ignore[arg-type]
                rowcount=99,
                description=desc,
                rows=[(1,), (2,)],
            )
        )
        assert acc.total_affected == 2
        assert len(acc.rows) == 2
        assert acc.description == desc

    def test_accumulates_rows_across_iterations(self) -> None:
        acc = _ExecuteManyAccumulator()
        desc = (("id", 1, None, None, None, None, None),)
        for batch in [[(1,)], [(2,), (3,)]]:
            acc.push(
                _FakeCursor(  # type: ignore[arg-type]
                    rowcount=len(batch),
                    description=desc,
                    rows=batch,  # type: ignore[arg-type]
                )
            )
        assert acc.total_affected == 3
        assert acc.rows == [(1,), (2,), (3,)]
        assert acc.description == desc


class TestPushMixed:
    def test_pure_dml_followed_by_returning_keeps_both_counts(self) -> None:
        """Some pathological drivers may switch mid-iteration — the
        branch split must handle each push independently."""
        acc = _ExecuteManyAccumulator()
        # First pure DML iteration: affected += 5
        acc.push(_FakeCursor(rowcount=5))  # type: ignore[arg-type]
        # Second RETURNING iteration: affected += len(rows)=2
        desc = (("id", 1, None, None, None, None, None),)
        acc.push(
            _FakeCursor(  # type: ignore[arg-type]
                rowcount=99,  # deliberately wrong; push must use len(rows)
                description=desc,
                rows=[(10,), (11,)],
            )
        )
        assert acc.total_affected == 7
        assert acc.rows == [(10,), (11,)]
        assert acc.description == desc
