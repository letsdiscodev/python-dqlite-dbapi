"""AsyncConnection.row_factory / text_factory getters.

text_factory is a stdlib-parity stub returning str unconditionally (dqlite has no hook).
"""

from __future__ import annotations

from dqlitedbapi.aio import AsyncConnection


def test_aconn_row_factory_getter_returns_set_value() -> None:
    """row_factory getter returns the value last assigned (None by default)."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        assert aconn.row_factory is None

        def factory(cur: object, row: tuple[object, ...]) -> dict[str, object]:
            return dict(zip(("a", "b"), row, strict=False))

        # Bypass the setter (which enforces loop binding) — pure getter pin.
        aconn._row_factory = factory
        assert aconn.row_factory is factory
        assert aconn.row_factory(None, (1, 2)) == {"a": 1, "b": 2}
    finally:
        aconn.force_close_transport()


def test_aconn_text_factory_getter_returns_str() -> None:
    """text_factory getter is the stdlib-parity stub returning str unconditionally."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        assert aconn.text_factory is str
    finally:
        aconn.force_close_transport()
