"""Pin: the four dbapi reprs route ``_address`` through ``sanitize_for_log`` before
``!r`` so control/bidi/zero-width/line-sep codepoints render as ``?`` everywhere,
matching the wire layer (not a mix of ``?`` and Python's ``\\uXXXX`` escape)."""

from __future__ import annotations

import weakref
from typing import Any

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


def _stub_sync_connection(address: str) -> Connection:
    conn = Connection.__new__(Connection)
    conn._address = address
    conn._database = "main"
    conn._closed = False
    conn._async_conn = None
    return conn


def _stub_async_connection(address: str) -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = address
    conn._database = "main"
    conn._closed = False
    conn._async_conn = None
    return conn


class _StubConn:
    """Parent-connection stand-in: cursor reads only ``_address`` from ``_connection``."""

    def __init__(self, address: str) -> None:
        self._address = address


def _stub_sync_cursor(address: str) -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rowcount = -1
    cur._connection = _StubConn(address)  # type: ignore[assignment]
    return cur


def _stub_async_cursor(address: str) -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rowcount = -1
    cur._connection = _StubConn(address)  # type: ignore[assignment]
    return cur


# Invisible literals below: U+2028 LINE SEPARATOR, U+202E RLO, U+200B ZWSP.
_LINE_SEP = " "
_RLO = "‮"
_ZWSP = "​"


def _all_reprs_for(address: str) -> list[str]:
    return [
        repr(_stub_sync_connection(address)),
        repr(_stub_async_connection(address)),
        repr(_stub_sync_cursor(address)),
        repr(_stub_async_cursor(address)),
    ]


def test_repr_substitutes_line_separator() -> None:
    """U+2028 must render as the sanitiser's ``?`` substitution on every dbapi repr."""
    address = f"leader{_LINE_SEP}forged:9001"
    for rendered in _all_reprs_for(address):
        assert _LINE_SEP not in rendered, (
            f"raw U+2028 leaked through dbapi repr (sibling-discipline "
            f"gap with wire layer): {rendered!r}"
        )
        # Sanitiser substitutes ``?`` before repr(), so the ``\\u2028`` escape is absent too.
        assert "\\u2028" not in rendered, (
            f"dbapi repr emitted ``\\u2028`` escape rather than the "
            f"sanitiser's ``?`` substitution; sibling parity with the "
            f"wire layer's ``?`` rendering broken: {rendered!r}"
        )
        assert "?" in rendered, f"dbapi repr lost the sanitiser's ``?`` substitution: {rendered!r}"


def test_repr_substitutes_bidi_and_zero_width() -> None:
    """RLO and ZWSP must also render as ``?`` on every dbapi repr."""
    address = f"left{_RLO}{_ZWSP}right:9001"
    for rendered in _all_reprs_for(address):
        assert _RLO not in rendered, f"raw U+202E leaked: {rendered!r}"
        assert _ZWSP not in rendered, f"raw U+200B leaked: {rendered!r}"
        assert "\\u202e" not in rendered, (
            f"dbapi repr emitted ``\\u202e`` escape rather than the sanitiser's ``?``: {rendered!r}"
        )
        assert "\\u200b" not in rendered, (
            f"dbapi repr emitted ``\\u200b`` escape rather than the sanitiser's ``?``: {rendered!r}"
        )


def test_repr_passes_through_safe_subset() -> None:
    """ASCII host:port survives the sanitiser as a no-op."""
    address = "127.0.0.1:9001"
    for rendered in _all_reprs_for(address):
        assert "127.0.0.1:9001" in rendered, (
            f"safe ASCII address mangled by sanitiser: {rendered!r}"
        )


def test_cursor_repr_tolerates_mock_connection_without_address() -> None:
    """The ``_address`` fallback to ``?`` for mock connections still works."""

    class _NoAddrConn:
        pass

    for cur_cls in (Cursor, AsyncCursor):
        cur: Any = cur_cls.__new__(cur_cls)
        cur._closed = False
        cur._rowcount = -1
        cur._connection = _NoAddrConn()
        rendered = repr(cur)
        assert "'?'" in rendered, (
            f"{cur_cls.__name__}.__repr__ lost the ``?`` fallback for "
            f"mocks lacking ``_address``: {rendered!r}"
        )


# Keep the weakref import (parity with other repr tests); suppress unused warning.
_ = weakref
