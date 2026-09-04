"""Bad ``max_total_rows`` raises ``ProgrammingError``: the dbapi wraps the
client layer's raw ``TypeError``/``ValueError`` into the PEP 249 ``Error`` hierarchy."""

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
