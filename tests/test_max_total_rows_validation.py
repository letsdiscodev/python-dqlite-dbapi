"""Validation of the ``max_total_rows`` constructor parameter (DB-API)."""

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio import AsyncConnection


class TestConnectionValidation:
    def test_zero_rejected(self) -> None:
        with pytest.raises(ValueError):
            Connection("localhost:19001", max_total_rows=0)

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValueError):
            Connection("localhost:19001", max_total_rows=-10)

    def test_bool_rejected(self) -> None:
        with pytest.raises(TypeError):
            Connection("localhost:19001", max_total_rows=True)  # type: ignore[arg-type]

    def test_string_rejected(self) -> None:
        with pytest.raises(TypeError):
            Connection("localhost:19001", max_total_rows="100")  # type: ignore[arg-type]

    def test_none_allowed(self) -> None:
        conn = Connection("localhost:19001", max_total_rows=None)
        assert conn._max_total_rows is None


class TestAsyncConnectionValidation:
    def test_zero_rejected(self) -> None:
        with pytest.raises(ValueError):
            AsyncConnection("localhost:19001", max_total_rows=0)

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValueError):
            AsyncConnection("localhost:19001", max_total_rows=-10)

    def test_bool_rejected(self) -> None:
        with pytest.raises(TypeError):
            AsyncConnection("localhost:19001", max_total_rows=True)  # type: ignore[arg-type]

    def test_none_allowed(self) -> None:
        conn = AsyncConnection("localhost:19001", max_total_rows=None)
        assert conn._max_total_rows is None
