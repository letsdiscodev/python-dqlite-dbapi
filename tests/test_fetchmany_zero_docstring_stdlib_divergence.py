"""``fetchmany`` docstrings flag the psycopg3 size=0 divergence, not a (stale) stdlib one.

stdlib sqlite3 on >=3.13 returns ``[]`` on ``fetchmany(0)`` like dqlite; the only live
divergence is psycopg3, which treats 0 as "use ``self.arraysize``"."""

from __future__ import annotations

import dqlitedbapi
import dqlitedbapi.aio


def test_sync_fetchmany_docstring_pins_correct_cross_driver_matrix() -> None:
    doc = (dqlitedbapi.Cursor.fetchmany.__doc__ or "").lower()
    assert "size=0" in doc or "size == 0" in doc, (
        "fetchmany docstring should document the size=0 corner case "
        "explicitly so the divergence callout has a referent."
    )
    assert "psycopg" in doc, (
        "fetchmany docstring must flag the psycopg3 divergence "
        "(psycopg3 treats size=0 as 'use self.arraysize')"
    )
    assert "differs from stdlib" not in doc and "diverges from stdlib" not in doc, (
        "fetchmany docstring must NOT claim a stdlib divergence — "
        "empirically stdlib 3.13+ returns [] on fetchmany(0), matching dqlite"
    )
    assert "stdlib sqlite3 parity" not in doc, (
        "fetchmany docstring should not perpetuate the old 'stdlib sqlite3 parity' claim wording"
    )
