"""_CODE_TO_EXCEPTION must route every wire BARE_DATABASE_ERROR_CODES member to bare
DatabaseError, keeping the dbapi's exception-class routing aligned with the SQLAlchemy
adapter's slot-fatal classification (both derived from the same wire SSOT). The dbapi's
bare-routes are a superset: they keep extra defensive pass-through entries.
"""

from __future__ import annotations

from dqlitedbapi.cursor import _CODE_TO_EXCEPTION
from dqlitedbapi.exceptions import DatabaseError
from dqlitewire import BARE_DATABASE_ERROR_CODES


def test_dbapi_bare_dbe_routes_match_wire_set() -> None:
    """Every wire BARE_DATABASE_ERROR_CODES member routes to bare DatabaseError in the dbapi."""
    routed_bare = {code for code, exc in _CODE_TO_EXCEPTION.items() if exc is DatabaseError}
    assert routed_bare >= BARE_DATABASE_ERROR_CODES, (
        f"dbapi _CODE_TO_EXCEPTION must route every wire "
        f"BARE_DATABASE_ERROR_CODES member to bare DatabaseError. "
        f"Missing: {BARE_DATABASE_ERROR_CODES - routed_bare}"
    )


def test_dbapi_keeps_defensive_passthrough_entries() -> None:
    """The dbapi keeps defensive pass-through entries (NOLFS/AUTH/NOTICE/WARNING) beyond the
    wire set; pin the asymmetry so a refactor collapsing to exactly the wire set surfaces."""
    from dqlitewire import SQLITE_AUTH, SQLITE_NOLFS, SQLITE_NOTICE, SQLITE_WARNING

    routed_bare = {code for code, exc in _CODE_TO_EXCEPTION.items() if exc is DatabaseError}
    for code in (SQLITE_NOLFS, SQLITE_AUTH, SQLITE_NOTICE, SQLITE_WARNING):
        assert code in routed_bare, (
            f"defensive pass-through code {code} must remain routed to "
            f"DatabaseError so a future server emission of it does not "
            f"surface as an unclassified OperationalError"
        )
