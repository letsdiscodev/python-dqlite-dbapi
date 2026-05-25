"""Pin: ``Cursor.executemany``'s docstring documents modern stdlib
(Python 3.10+) ``lastrowid`` behaviour, NOT the historical "left
unchanged" framing that was a behaviour shift, not a current
discipline.

Modern stdlib ``sqlite3.Cursor.executemany`` updates ``lastrowid``
per iteration; this driver clears to ``None`` on the non-empty
batch path rather than picking an arbitrary last-iteration row.
The empty-batch path preserves the pre-batch snapshot — a
deliberate divergence that the docstring must call out.
"""

from __future__ import annotations

from dqlitedbapi.cursor import Cursor


def test_executemany_docstring_mentions_modern_stdlib() -> None:
    """The docstring must reference the modern (Python 3.10+)
    stdlib behaviour, NOT the stale "left unchanged" framing.
    """
    docstring = Cursor.executemany.__doc__
    assert docstring is not None
    assert "Python 3.10+" in docstring or "modern stdlib" in docstring, (
        f"executemany docstring must reference modern stdlib behaviour; got: {docstring!r}"
    )
    # The stale framing should not be the primary claim.
    # ("left unchanged" can appear contextually but not as a stdlib claim.)
    assert "stdlib's documented contract is" not in docstring


def test_executemany_docstring_documents_empty_batch_divergence() -> None:
    """The empty-batch path preserves the pre-batch snapshot; that
    divergence from modern stdlib (which clears even on empty)
    must be called out so cross-driver porters see it.
    """
    docstring = Cursor.executemany.__doc__
    assert docstring is not None
    # The empty-batch divergence is documented explicitly.
    assert "empty" in docstring.lower()
    assert "pre-batch" in docstring or "divergence" in docstring
