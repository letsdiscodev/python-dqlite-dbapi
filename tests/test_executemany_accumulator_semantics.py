"""``_ExecuteManyAccumulator.push`` decouples rows-returned vs rows-affected."""

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
        """A stray rowcount=-1 ("not determinable") must not decrement the total."""
        acc = _ExecuteManyAccumulator()
        acc.push(_FakeCursor(rowcount=2))  # type: ignore[arg-type]
        acc.push(_FakeCursor(rowcount=-1))  # type: ignore[arg-type]
        assert acc.total_affected == 2


class TestPushRowReturning:
    def test_total_affected_uses_len_rows_not_rowcount(self) -> None:
        """On the RETURNING branch total_affected sums len(rows), not _rowcount."""
        acc = _ExecuteManyAccumulator()
        desc = (("id", 1, None, None, None, None, None),)
        # Cursor lies: _rowcount=99 but 2 rows; push must use len(rows)=2.
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
        """A driver switching DML<->RETURNING mid-iteration: each push is independent."""
        acc = _ExecuteManyAccumulator()
        acc.push(_FakeCursor(rowcount=5))  # type: ignore[arg-type]
        desc = (("id", 1, None, None, None, None, None),)
        acc.push(
            _FakeCursor(  # type: ignore[arg-type]
                rowcount=99,  # wrong on purpose; push must use len(rows)
                description=desc,
                rows=[(10,), (11,)],
            )
        )
        assert acc.total_affected == 7
        assert acc.rows == [(10,), (11,)]
        assert acc.description == desc
