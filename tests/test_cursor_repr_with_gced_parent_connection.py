"""Pin: ``Cursor.__repr__`` does NOT leak ``ReferenceError`` when
the parent connection has been garbage-collected.

After ``Cursor.close()``, ``self._connection`` is swapped to a
``weakref.proxy``. When the parent ``Connection`` is later
GC'd, every attribute access on the proxy raises
``ReferenceError`` — and ``__repr__`` reads
``getattr(self._connection, "_address", "?")`` WITHOUT
catching ``ReferenceError``. The getattr default fires only when
the attribute is missing; the proxy raises BEFORE the default
is consulted.

``ReferenceError`` is outside the ``dbapi.Error`` hierarchy — every
``except dbapi.Error:`` block misses it. ``repr()`` is called by
debuggers / loggers / pytest output, so the leak crashes debugging
tooling.

The sibling ``Cursor.connection`` property (cursor.py:1269-1281)
already defends against the GC'd-proxy case with the same fallback;
``__repr__`` should mirror the discipline.
"""

from __future__ import annotations

import gc
import weakref
from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def _make_cursor() -> tuple[Cursor, MagicMock]:
    cur = Cursor.__new__(Cursor)
    parent = MagicMock()
    parent._address = "localhost:9001"
    parent._check_thread = MagicMock()
    parent._max_total_rows = None
    cur._connection = parent
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur._row_factory = None
    cur._completed_iterations = 0
    cur.messages = []
    return cur, parent


def test_cursor_repr_does_not_leak_referenceerror_on_gced_parent() -> None:
    """Setup: close the cursor (which swaps _connection to a
    ``weakref.proxy``). Drop the strong ref to the parent and force
    GC. ``repr(cur)`` must not raise.
    """
    cur, parent = _make_cursor()
    cur.close()
    # After close(), _connection is a weakref.proxy of `parent`.
    cur._connection = weakref.proxy(parent)  # type: ignore[assignment]
    # Drop the strong ref and force GC so the proxy goes stale.
    del parent
    gc.collect()
    # The contract: repr() must NOT raise ReferenceError.
    s = repr(cur)
    # The output should still mention "closed" and "Cursor".
    assert "Cursor" in s
    assert "closed" in s
    # Without the fix, repr() raises ReferenceError here.


def test_cursor_repr_address_falls_back_to_placeholder_when_proxy_dead() -> None:
    """The fallback for a stale-proxy address read should be the
    same ``'?'`` placeholder the existing missing-attribute path
    uses. This pins the exact fallback shape so future code does
    not regress to e.g. ``<repr-error>`` or empty string.
    """
    cur, parent = _make_cursor()
    cur.close()
    cur._connection = weakref.proxy(parent)  # type: ignore[assignment]
    del parent
    gc.collect()
    s = repr(cur)
    # The address slot rendered as '?' (the canonical placeholder).
    assert "address='?'" in s or 'address="?"' in s
