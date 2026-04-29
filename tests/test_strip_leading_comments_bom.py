"""Pin: ``_strip_leading_comments`` strips a leading UTF-8 BOM
(``\\ufeff``) so SQL imported via PowerShell ``Set-Content``,
Notepad, or read with ``encoding='utf-8'`` (instead of
``encoding='utf-8-sig'``) is classified correctly.

SQLite's ``prepare.c::sqlite3_prepare_v2`` skips a leading BOM
before tokenisation. Python's ``str.strip()`` does NOT consider
``\\ufeff`` whitespace (``'\\ufeff'.isspace()`` is False), so the
classifier helpers (which start with ``s.strip()``) would otherwise
miss ``\\ufeffSELECT`` / ``\\ufeffBEGIN`` etc.

Two duplicate helpers exist:
- ``dqlitedbapi/cursor.py:_strip_leading_comments`` (also imported
  by ``aio/cursor.py``)
- ``dqliteclient/connection.py:_strip_leading_comments``

Both must strip the BOM. A parity test in this file pins both copies.
"""

from __future__ import annotations

from dqlitedbapi.cursor import _is_row_returning, _strip_leading_comments

_BOM = "﻿"


class TestStripLeadingCommentsStripsBom:
    def test_strips_lone_bom(self) -> None:
        assert _strip_leading_comments(f"{_BOM}SELECT 1") == "SELECT 1"

    def test_strips_bom_then_whitespace(self) -> None:
        assert _strip_leading_comments(f"{_BOM}   SELECT 1") == "SELECT 1"

    def test_strips_bom_then_line_comment(self) -> None:
        assert _strip_leading_comments(f"{_BOM}-- hi\nSELECT 1") == "SELECT 1"

    def test_strips_bom_then_block_comment(self) -> None:
        assert _strip_leading_comments(f"{_BOM}/* hi */ SELECT 1") == "SELECT 1"

    def test_no_bom_unchanged(self) -> None:
        assert _strip_leading_comments("SELECT 1") == "SELECT 1"


class TestClassifierRecognisesBomPrefixedSelect:
    def test_is_row_returning_with_bom_prefixed_select(self) -> None:
        assert _is_row_returning(f"{_BOM}SELECT 1") is True


class TestClientHelperParity:
    """Both copies of ``_strip_leading_comments`` must behave
    identically on BOM input — drift would re-introduce the
    classifier-desync defect on one side or the other."""

    def test_client_and_dbapi_agree_on_bom(self) -> None:
        from dqliteclient.connection import (
            _strip_leading_comments as client_strip,
        )

        for sql in [
            f"{_BOM}SELECT 1",
            f"{_BOM}   SELECT 1",
            f"{_BOM}-- hi\nSELECT 1",
            f"{_BOM}/* hi */ SELECT 1",
            f"{_BOM}BEGIN",
            f"{_BOM}COMMIT",
            f"{_BOM}SAVEPOINT foo",
        ]:
            assert _strip_leading_comments(sql) == client_strip(sql), (
                f"client and dbapi helpers diverged on {sql!r}"
            )
