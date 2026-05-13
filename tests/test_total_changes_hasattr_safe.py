"""Pin: ``hasattr(conn, "total_changes")`` does not raise.

``total_changes`` is part of the family of stdlib ``sqlite3``-parity
stubs that raise ``NotSupportedError`` when called. The rest of the
family are methods, so ``hasattr`` returns True (``getattr`` returns
the bound method without invoking it). ``total_changes`` was the
lone ``@property`` outlier — accessing the attribute invoked the
descriptor's getter, which raised ``NotSupportedError``, which
``hasattr`` (in Python 3.2+) propagates because it only catches
``AttributeError``.

The fix converts the ``@property`` to a method. Stdlib
``sqlite3.Connection.total_changes`` is attribute-style; the project
deliberately favours the "``hasattr`` always returns True for stubs"
invariant over property-style fidelity. Documented divergence.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import NotSupportedError


def test_hasattr_total_changes_sync_does_not_raise() -> None:
    """``hasattr`` is documented to never raise (it catches
    ``AttributeError`` only). With ``total_changes`` as a method,
    ``getattr(conn, "total_changes")`` returns the bound method
    without invoking the stub, so ``hasattr`` returns True cleanly."""
    conn = dqlitedbapi.Connection("localhost:9001", timeout=1.0)
    try:
        try:
            present = hasattr(conn, "total_changes")
        except NotSupportedError:
            pytest.fail(
                "hasattr() leaked NotSupportedError — total_changes must be a "
                "method-stub like the rest of the family, not a @property"
            )
        assert present is True
        # And calling the stub still raises (parens — method form).
        with pytest.raises(NotSupportedError, match="total_changes"):
            conn.total_changes()
    finally:
        conn._closed = True


def test_hasattr_total_changes_async_does_not_raise() -> None:
    """Async sibling. AsyncConnection's ``total_changes`` was also a
    ``@property`` outlier."""
    aconn = AsyncConnection("localhost:9001", timeout=1.0)
    try:
        present = hasattr(aconn, "total_changes")
    except NotSupportedError:
        pytest.fail(
            "hasattr() leaked NotSupportedError on AsyncConnection — "
            "total_changes must be a method-stub like the rest of the family"
        )
    assert present is True
    with pytest.raises(NotSupportedError, match="total_changes"):
        aconn.total_changes()
