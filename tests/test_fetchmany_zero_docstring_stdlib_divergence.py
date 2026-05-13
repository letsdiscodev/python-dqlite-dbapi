"""Doc-pin: ``Cursor.fetchmany`` / ``AsyncCursor.fetchmany`` docstrings
do NOT claim "stdlib sqlite3 parity" for the ``size=0`` corner case.

The prior docstring asserted "stdlib sqlite3 parity" for the
``size=0`` corner; that claim has shifted across CPython releases
(older Python releases drained the result set on ``fetchmany(0)``,
recent releases return ``[]``) and was never a guarantee dqlite
could meaningfully make. The replacement wording drops the
parity claim and explicitly documents the three-driver matrix —
stdlib behaviour varies / psycopg3 treats ``0`` as "use arraysize"
/ dqlite returns ``[]`` deterministically.

A doc-pin is sufficient because there is no behaviour change — this
issue is documentation-only. The behaviour pin lives at
``test_fetchmany_edges.py``; here we only assert that the docstring
does not perpetuate the false-parity claim and explicitly flags the
divergence.
"""

from __future__ import annotations

import dqlitedbapi
import dqlitedbapi.aio


def test_sync_fetchmany_docstring_drops_stdlib_parity_claim() -> None:
    doc = (dqlitedbapi.Cursor.fetchmany.__doc__ or "").lower()
    assert "size=0" in doc or "size == 0" in doc, (
        "fetchmany docstring should document the size=0 corner case "
        "explicitly so the divergence callout has a referent."
    )
    # The old wording said "stdlib sqlite3 parity" for size=0 which is
    # factually false / version-dependent. The new wording must
    # explicitly flag the divergence.
    assert "differs from stdlib" in doc or "diverges from stdlib" in doc, (
        "fetchmany docstring must explicitly flag the stdlib divergence "
        "for size=0 — stdlib's behaviour is version-dependent and was "
        "never a reliable parity claim."
    )
    # Defensive: the literal "stdlib sqlite3 parity" substring (with
    # the conjoined wording) should no longer appear.
    assert "stdlib sqlite3 parity" not in doc, (
        "fetchmany docstring still contains the 'stdlib sqlite3 parity' "
        "phrase that this fix was supposed to remove."
    )


def test_async_fetchmany_docstring_drops_stdlib_parity_claim() -> None:
    doc = (dqlitedbapi.aio.AsyncCursor.fetchmany.__doc__ or "").lower()
    assert "size=0" in doc or "size == 0" in doc, (
        "AsyncCursor.fetchmany docstring should document the size=0 corner case."
    )
    assert "differs from stdlib" in doc or "diverges from stdlib" in doc, (
        "AsyncCursor.fetchmany docstring must explicitly flag the stdlib "
        "divergence for size=0 (mirrors the sync sibling)."
    )
    assert "stdlib sqlite3 parity" not in doc, (
        "AsyncCursor.fetchmany docstring still contains the 'stdlib sqlite3 "
        "parity' phrase that this fix was supposed to remove."
    )
