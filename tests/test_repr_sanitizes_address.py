"""Sibling-discipline pin: the four dbapi reprs route ``_address``
through ``sanitize_for_log`` before ``!r`` so attacker-influenced
control / bidi / zero-width / line-separator codepoints render as
operator-readable ``?`` rather than the cosmetically-different
``\\uXXXX`` six-char escape Python's ``str.__repr__`` produces.

The reviewer noted Python ``str.__repr__`` already escapes U+2028
to ``\\u2028`` so the journald-record-splitting CWE-117 exploit window
the original finding framed does NOT exist for this surface. The
fix is sibling-parity / log-reader UX — every layer of the stack
renders the same ``?`` substitution everywhere the address appears
in logs, not a mix of ``?`` (wire layer) and ``\\u2028`` (dbapi
layer) for the same bytes.

Pinned reprs:
- ``Connection.__repr__``
- ``AsyncConnection.__repr__``
- ``Cursor.__repr__``
- ``AsyncCursor.__repr__``
"""

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
    """Stand-in for a parent connection on the cursor repr path —
    the cursor only reads ``getattr(self._connection, "_address",
    "?")`` from its ``_connection`` slot."""

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


# U+2028 LINE SEPARATOR is the canonical journald-splitting codepoint
# the wire-layer fix targeted. U+202E (RLO) flips render direction;
# U+200B (ZWSP) is zero-width.
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
    """U+2028 (the wire-layer fix's headline codepoint) must render
    as the sanitiser's ``?`` substitution on every dbapi repr."""
    address = f"leader{_LINE_SEP}forged:9001"
    for rendered in _all_reprs_for(address):
        # The sanitised form has the ``?`` substitution; the raw
        # codepoint does NOT appear.
        assert _LINE_SEP not in rendered, (
            f"raw U+2028 leaked through dbapi repr (sibling-discipline "
            f"gap with wire layer): {rendered!r}"
        )
        # Python ``str.__repr__`` would emit ``\\u2028`` six-char
        # escape; the sanitiser substitutes ``?`` BEFORE ``repr()``
        # is applied, so the escape sequence is also absent.
        assert "\\u2028" not in rendered, (
            f"dbapi repr emitted ``\\u2028`` escape rather than the "
            f"sanitiser's ``?`` substitution; sibling parity with the "
            f"wire layer's ``?`` rendering broken: {rendered!r}"
        )
        # Operator-readable ``?`` IS present.
        assert "?" in rendered, f"dbapi repr lost the sanitiser's ``?`` substitution: {rendered!r}"


def test_repr_substitutes_bidi_and_zero_width() -> None:
    """RLO and ZWSP — the other invisible-class codepoints the wire
    layer sanitises — must also render as ``?`` on every dbapi repr."""
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
    """The ``getattr(..., "_address", "?")`` fallback for mock-backed
    test fixtures still works — the sanitiser is a no-op on ``"?"``."""

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


# Suppress unused-import warnings on weakref — kept for parity with
# other repr tests in case the stub helpers grow weakref-related
# state later.
_ = weakref
