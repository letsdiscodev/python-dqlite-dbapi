"""Pin: dqlite-namespace error codes (DQLITE_PROTO=1001,
DQLITE_NOTFOUND=1002, DQLITE_PARSE=1005) route to the PEP 249
exception classes their semantics deserve, not the OperationalError
default. Upstream emission sites:

- ``gateway.c`` emits DQLITE_PROTO for protocol misuse (e.g.
  "unrecognized request type") → ``InterfaceError``.
- ``gateway.c::handle_request_open`` emits DQLITE_NOTFOUND for
  "database does not exists" → ``ProgrammingError`` (config typo).
- ``gateway.c`` emits DQLITE_PARSE for schema-version mismatch /
  unrecognised cluster format → ``ProgrammingError``.

The previous behaviour collapsed all three into ``OperationalError``,
which caused portable retry logic to retry forever on a config typo.
"""

from __future__ import annotations

from dqlitedbapi import (
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from dqlitedbapi.cursor import _classify_operational


class TestDqliteNamespaceCodeClassification:
    def test_dqlite_proto_maps_to_interface_error(self) -> None:
        assert _classify_operational(1001) is InterfaceError

    def test_dqlite_notfound_maps_to_programming_error(self) -> None:
        assert _classify_operational(1002) is ProgrammingError

    def test_dqlite_parse_maps_to_programming_error(self) -> None:
        assert _classify_operational(1005) is ProgrammingError

    def test_unmapped_code_still_falls_back_to_operational_error(self) -> None:
        # An unknown code (e.g. a future SQLite extended primary not in
        # the dispatch table) still defaults to OperationalError.
        assert _classify_operational(99) is OperationalError

    def test_none_code_still_falls_back_to_operational_error(self) -> None:
        assert _classify_operational(None) is OperationalError

    def test_sqlite_primary_codes_unaffected(self) -> None:
        # Regression guard: the dqlite-namespace additions did not
        # disturb the SQLite-primary entries.
        from dqlitedbapi import IntegrityError

        # SQLITE_CONSTRAINT_UNIQUE = 19 | (8 << 8) = 2067; primary 19
        assert _classify_operational(2067) is IntegrityError
        assert _classify_operational(19) is IntegrityError
