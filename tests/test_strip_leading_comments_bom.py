"""``_strip_leading_comments`` strips a leading UTF-8 BOM, which str.strip() leaves intact
(``'\\ufeff'.isspace()`` is False). Both duplicate helpers (dbapi + client) must match."""

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
    """Both copies must behave identically on BOM input; drift re-introduces desync."""

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
