"""Doc-pin: ``Cursor.fetchmany`` / ``AsyncCursor.fetchmany`` docstrings
correctly describe the ``size=0`` cross-driver matrix.

Empirically on the supported Python floor (>=3.13), stdlib sqlite3
returns ``[]`` without consuming rows on ``fetchmany(0)`` — same as
dqlite. The earlier docstring claim of a stdlib divergence was
stale residue from older Python compatibility windows where stdlib's
behaviour was version-dependent. The active divergence is with
psycopg3 (which treats ``0`` as "use ``self.arraysize``") and that
remains documented.

The pinning here is doc-only: behaviour pin lives elsewhere
(``test_fetchmany_edges.py``). This test enforces that the docstring
does NOT perpetuate the stale stdlib-divergence claim and DOES
flag the real psycopg3 divergence.
"""

from __future__ import annotations

import dqlitedbapi
import dqlitedbapi.aio


def test_sync_fetchmany_docstring_pins_correct_cross_driver_matrix() -> None:
    doc = (dqlitedbapi.Cursor.fetchmany.__doc__ or "").lower()
    assert "size=0" in doc or "size == 0" in doc, (
        "fetchmany docstring should document the size=0 corner case "
        "explicitly so the divergence callout has a referent."
    )
    # The active divergence is with psycopg3, NOT stdlib.
    assert "psycopg" in doc, (
        "fetchmany docstring must flag the psycopg3 divergence "
        "(psycopg3 treats size=0 as 'use self.arraysize')"
    )
    # The stale "differs from stdlib" / "diverges from stdlib"
    # claim MUST be gone — stdlib 3.13+ matches dqlite.
    assert "differs from stdlib" not in doc and "diverges from stdlib" not in doc, (
        "fetchmany docstring must NOT claim a stdlib divergence — "
        "empirically stdlib 3.13+ returns [] on fetchmany(0), matching dqlite"
    )
    # Defensive: the literal "stdlib sqlite3 parity" substring (with
    # the conjoined wording) should not appear either way.
    assert "stdlib sqlite3 parity" not in doc, (
        "fetchmany docstring should not perpetuate the old 'stdlib sqlite3 parity' claim wording"
    )


def test_async_fetchmany_docstring_pins_correct_cross_driver_matrix() -> None:
    doc = (dqlitedbapi.aio.AsyncCursor.fetchmany.__doc__ or "").lower()
    assert "size=0" in doc or "size == 0" in doc, (
        "AsyncCursor.fetchmany docstring should document the size=0 corner case."
    )
    assert "psycopg" in doc, "AsyncCursor.fetchmany docstring must flag the psycopg3 divergence"
    assert "differs from stdlib" not in doc and "diverges from stdlib" not in doc, (
        "AsyncCursor.fetchmany docstring must NOT claim a stdlib divergence"
    )
    assert "stdlib sqlite3 parity" not in doc, (
        "AsyncCursor.fetchmany docstring should not perpetuate the old "
        "'stdlib sqlite3 parity' claim wording"
    )
