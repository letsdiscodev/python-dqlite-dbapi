"""``Cursor.executemany``'s docstring documents modern-stdlib lastrowid behaviour."""

from __future__ import annotations

from dqlitedbapi.cursor import Cursor


def test_executemany_docstring_documents_empty_batch_divergence() -> None:
    """The docstring calls out the empty-batch divergence from modern stdlib."""
    docstring = Cursor.executemany.__doc__
    assert docstring is not None
    assert "empty" in docstring.lower()
    assert "pre-batch" in docstring or "divergence" in docstring
