"""dqlite-namespace codes route to semantic PEP 249 classes, not the OperationalError
default: DQLITE_PROTO=1001 -> InterfaceError, DQLITE_NOTFOUND=1002 and DQLITE_PARSE=1005
-> ProgrammingError. The old OperationalError collapse made retry logic loop on config typos.
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
        assert _classify_operational(99) is OperationalError

    def test_none_code_still_falls_back_to_operational_error(self) -> None:
        assert _classify_operational(None) is OperationalError

    def test_sqlite_primary_codes_unaffected(self) -> None:
        from dqlitedbapi import IntegrityError

        # SQLITE_CONSTRAINT_UNIQUE = 19 | (8 << 8) = 2067; primary 19.
        assert _classify_operational(2067) is IntegrityError
        assert _classify_operational(19) is IntegrityError
