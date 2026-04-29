"""Validation of the ``max_total_rows`` constructor parameter (DB-API).

The dbapi layer is the PEP 249 §7 boundary: every error originating
from the driver must be a subclass of ``Error``. Bad
``max_total_rows`` values therefore raise ``ProgrammingError`` (the
PEP 249 class for "errors related to the database's operation, but
not necessarily under the control of the programmer", per §6.1.4).
The client-layer validator still raises raw ``TypeError`` /
``ValueError`` for client-only consumers (per ISSUE-39); the dbapi
entry points wrap.
"""

import pytest

from dqlitedbapi import Connection, ProgrammingError
from dqlitedbapi.aio import AsyncConnection


class TestConnectionValidation:
    def test_zero_rejected(self) -> None:
        with pytest.raises(ProgrammingError):
            Connection("localhost:19001", max_total_rows=0)

    def test_negative_rejected(self) -> None:
        with pytest.raises(ProgrammingError):
            Connection("localhost:19001", max_total_rows=-10)

    def test_bool_rejected(self) -> None:
        with pytest.raises(ProgrammingError):
            Connection("localhost:19001", max_total_rows=True)

    def test_string_rejected(self) -> None:
        with pytest.raises(ProgrammingError):
            Connection("localhost:19001", max_total_rows="100")  # type: ignore[arg-type]

    def test_none_allowed(self) -> None:
        conn = Connection("localhost:19001", max_total_rows=None)
        assert conn._max_total_rows is None


class TestAsyncConnectionValidation:
    def test_zero_rejected(self) -> None:
        with pytest.raises(ProgrammingError):
            AsyncConnection("localhost:19001", max_total_rows=0)

    def test_negative_rejected(self) -> None:
        with pytest.raises(ProgrammingError):
            AsyncConnection("localhost:19001", max_total_rows=-10)

    def test_bool_rejected(self) -> None:
        with pytest.raises(ProgrammingError):
            AsyncConnection("localhost:19001", max_total_rows=True)

    def test_none_allowed(self) -> None:
        conn = AsyncConnection("localhost:19001", max_total_rows=None)
        assert conn._max_total_rows is None
