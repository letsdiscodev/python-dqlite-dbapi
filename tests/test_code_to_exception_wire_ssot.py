"""Pin: ``dqlitedbapi`` ``_CODE_TO_EXCEPTION`` routes every wire-layer
``BARE_DATABASE_ERROR_CODES`` member to bare ``DatabaseError``.

The wire-layer ``BARE_DATABASE_ERROR_CODES`` frozenset
(``SQLITE_CORRUPT`` / ``SQLITE_FORMAT`` / ``SQLITE_NOTADB``) is the
SSOT for "slot-fatal codes that route to bare DatabaseError" — used
directly by the SQLAlchemy adapter
(``sqlalchemydqlite/base.py::_BARE_DBE_DISCONNECT_CODES``). The dbapi
table at ``dqlitedbapi.cursor._CODE_TO_EXCEPTION`` previously
hard-coded the three codes individually; if a future wire-set change
adds a slot-fatal code (or the dbapi table accidentally drops one),
the SQLAlchemy adapter's slot-fatal classification silently diverges
from the dbapi's exception-class routing.

This pin asserts the SSOT identity contract directly. The dbapi may
add extra defensive pass-through entries (NOLFS / AUTH / NOTICE /
WARNING per the dbapi cursor comment block); the wire set is the
SLOT-FATAL subset that dqlite-server actually emits. The dbapi's
bare-routes must therefore be a SUPERSET of the wire set.
"""

from __future__ import annotations

from dqlitedbapi.cursor import _CODE_TO_EXCEPTION
from dqlitedbapi.exceptions import DatabaseError
from dqlitewire import BARE_DATABASE_ERROR_CODES


def test_dbapi_bare_dbe_routes_match_wire_set() -> None:
    """Every member of the wire SSOT ``BARE_DATABASE_ERROR_CODES``
    must route to bare ``DatabaseError`` in the dbapi mapping —
    matching the slot-fatal classification SA's adapter derives from
    the same wire set."""
    routed_bare = {code for code, exc in _CODE_TO_EXCEPTION.items() if exc is DatabaseError}
    assert routed_bare >= BARE_DATABASE_ERROR_CODES, (
        f"dbapi _CODE_TO_EXCEPTION must route every wire "
        f"BARE_DATABASE_ERROR_CODES member to bare DatabaseError. "
        f"Missing: {BARE_DATABASE_ERROR_CODES - routed_bare}"
    )


def test_dbapi_keeps_defensive_passthrough_entries() -> None:
    """The dbapi's bare-DBE routes are a STRICT superset of the wire
    set — the extra defensive pass-through entries (NOLFS / AUTH /
    NOTICE / WARNING) are kept locally per the cursor comment block.

    This pin documents the asymmetry so a future refactor that
    accidentally collapsed the dbapi set to exactly the wire set
    (dropping the defensive entries) surfaces."""
    from dqlitewire import SQLITE_AUTH, SQLITE_NOLFS, SQLITE_NOTICE, SQLITE_WARNING

    routed_bare = {code for code, exc in _CODE_TO_EXCEPTION.items() if exc is DatabaseError}
    for code in (SQLITE_NOLFS, SQLITE_AUTH, SQLITE_NOTICE, SQLITE_WARNING):
        assert code in routed_bare, (
            f"defensive pass-through code {code} must remain routed to "
            f"DatabaseError so a future server emission of it does not "
            f"surface as an unclassified OperationalError"
        )
