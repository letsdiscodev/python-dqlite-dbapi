"""Pin the divergence: sync ``Connection.isolation_level`` and
``Connection.autocommit`` setters store on the SYNC WRAPPER's slot
only. They do NOT mirror to the inner ``AsyncConnection``'s
``_isolation_level_value`` / ``_autocommit_value`` slot.

Round-7 commit ``2453335`` enables ``dst.isolation_level =
src.isolation_level`` round-trip via the sync wrapper. The wire
layer no-ops every accepted value (dqlite is fixed-mode autocommit),
so divergence between the sync wrapper and the inner
``AsyncConnection`` has zero observable effect on SQL execution.

Threading the setter write across the loop-thread boundary to mirror
to the inner would introduce ordering hazards the rest of the sync
surface deliberately avoids. The deliberate non-mirror is pinned
here so a future refactor that accidentally couples the slots
without re-thinking the threading model surfaces fast.

If a future commit DOES decide to mirror, this test should be
updated alongside the docstring divergence callout in
``connection.py`` (search for "INDEPENDENT" near the setter).
"""

from __future__ import annotations

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def _connection_with_inner() -> tuple[Connection, AsyncConnection]:
    """Construct a sync Connection and attach an inner AsyncConnection
    with the default property slots so the divergence is observable.

    Constructed via ``__new__`` to avoid triggering ``_ensure_loop``
    and any wire-side side-effects; the property setters are pure
    Python state stores so the minimal attribute shape suffices.
    """
    conn = Connection("127.0.0.1:9999")
    inner = AsyncConnection.__new__(AsyncConnection)
    inner._closed = False
    # ``_async_conn`` is statically typed as ``DqliteConnection | None``;
    # this test uses an ``AsyncConnection`` as a stand-in for the
    # purpose of pinning the divergence — the property setters never
    # touch the inner so the static-type mismatch is benign.
    conn._async_conn = inner  # type: ignore[assignment]
    return conn, inner


def test_isolation_level_setter_does_not_mirror_to_inner() -> None:
    """Sync setter assigns ``self._isolation_level_value`` only; the
    inner ``AsyncConnection.isolation_level`` returns its own
    default (``None``) regardless of the outer's last input."""
    conn, inner = _connection_with_inner()
    try:
        conn.isolation_level = "DEFERRED"
        assert conn.isolation_level == "DEFERRED"
        # Inner's slot was never written; its getter falls back to
        # the documented default ``None``.
        assert getattr(inner, "_isolation_level_value", None) is None
    finally:
        conn._closed = True


def test_autocommit_setter_does_not_mirror_to_inner() -> None:
    """Sync setter assigns ``self._autocommit_value`` only; the
    inner ``AsyncConnection.autocommit`` returns its own default
    (``True``) regardless of the outer's last input."""
    conn, inner = _connection_with_inner()
    try:
        conn.autocommit = -1  # LEGACY_TRANSACTION_CONTROL sentinel
        assert conn.autocommit == -1
        # Inner's slot was never written.
        assert not hasattr(inner, "_autocommit_value")
    finally:
        conn._closed = True


def test_isolation_level_divergence_documented_in_setter() -> None:
    """Doc-pin: the divergence is called out in the setter source so
    future readers see the rationale at the site of the omission."""
    import inspect

    src = inspect.getsource(Connection.isolation_level.fset)  # type: ignore[attr-defined]
    # The pin-keyword the setter uses to call out the divergence —
    # mention of the inner async conn's slot being independent.
    assert "INDEPENDENT" in src or "independent" in src, (
        "isolation_level.setter no longer documents the inner-async-"
        "conn-slot divergence; either re-add the callout or update "
        "this pin alongside whatever coupling change motivated the "
        "removal."
    )
    assert "_async_conn" in src, (
        "isolation_level.setter docstring no longer references "
        "``_async_conn`` — the divergence callout has lost its anchor."
    )


def test_autocommit_divergence_documented_in_setter() -> None:
    """Doc-pin: same as above for the autocommit setter."""
    import inspect

    src = inspect.getsource(Connection.autocommit.fset)  # type: ignore[attr-defined]
    assert "INDEPENDENT" in src or "independent" in src, (
        "autocommit.setter no longer documents the inner-async-"
        "conn-slot divergence; either re-add the callout or update "
        "this pin alongside whatever coupling change motivated the "
        "removal."
    )
