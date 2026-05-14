"""Pin: ``AsyncConnection`` getter side for ``row_factory`` and
``text_factory``.

The setters are covered by ``test_aio_row_factory_setter_loop_binding.py``
and other suites, but the getters themselves (which return the current
state without mutation) had no direct unit pin. Both encode stdlib
parity contracts:

- ``row_factory`` returns the value last set (or ``None``).
- ``text_factory`` returns ``str`` unconditionally (stdlib-parity stub
  — dqlite has no equivalent SQLite text-handling hook).

Without these pins, a future refactor that changes ``text_factory`` to
track an actual per-connection setting would silently break the parity
stub contract, and a regression to ``row_factory`` (e.g. returning
``self._row_factory or self._default_factory`` mid-refactor) would not
surface.
"""

from __future__ import annotations

from dqlitedbapi.aio import AsyncConnection


def test_aconn_row_factory_getter_returns_set_value() -> None:
    """Pin: ``AsyncConnection.row_factory`` getter returns the value
    last assigned (and ``None`` by default)."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        # Default is None per stdlib parity.
        assert aconn.row_factory is None

        def factory(cur: object, row: tuple[object, ...]) -> dict[str, object]:
            return dict(zip(("a", "b"), row, strict=False))

        # Bypass the setter (which enforces loop binding) — this is a
        # pure getter pin.
        aconn._row_factory = factory
        assert aconn.row_factory is factory
        # The returned callable must still behave like the assigned one.
        assert aconn.row_factory(None, (1, 2)) == {"a": 1, "b": 2}
    finally:
        aconn.force_close_transport()


def test_aconn_text_factory_getter_returns_str() -> None:
    """Pin: ``AsyncConnection.text_factory`` getter is the stdlib-parity
    stub returning ``str`` unconditionally — dqlite has no equivalent
    SQLite text-handling hook."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        assert aconn.text_factory is str
    finally:
        aconn.force_close_transport()
