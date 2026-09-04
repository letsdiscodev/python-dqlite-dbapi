"""Both ``Connection.__init__``s wrap the client-layer validator's
``TypeError``/``ValueError`` into PEP 249 ``ProgrammingError`` (the dbapi
boundary requires every driver error to subclass ``dqlitedbapi.Error``)."""

from __future__ import annotations

import pytest

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


class TestSyncConnectionWrapsPositiveIntValidator:
    def test_negative_max_total_rows_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_total_rows"):
            Connection("127.0.0.1:9001", max_total_rows=-1)

    def test_zero_max_total_rows_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_total_rows"):
            Connection("127.0.0.1:9001", max_total_rows=0)

    def test_non_int_max_total_rows_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_total_rows"):
            Connection("127.0.0.1:9001", max_total_rows="oops")  # type: ignore[arg-type]

    def test_negative_max_continuation_frames_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_continuation_frames"):
            Connection("127.0.0.1:9001", max_continuation_frames=-1)

    def test_non_int_max_continuation_frames_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_continuation_frames"):
            Connection("127.0.0.1:9001", max_continuation_frames=[])  # type: ignore[arg-type]

    def test_valid_positive_int_accepted(self) -> None:
        c = Connection("127.0.0.1:9001", max_total_rows=100, max_continuation_frames=50)
        assert c._max_total_rows == 100
        assert c._max_continuation_frames == 50

    def test_none_accepted_for_both(self) -> None:
        """``None`` means "no cap"."""
        c = Connection("127.0.0.1:9001", max_total_rows=None, max_continuation_frames=None)
        assert c._max_total_rows is None
        assert c._max_continuation_frames is None


class TestAsyncConnectionWrapsPositiveIntValidator:
    def test_negative_max_total_rows_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_total_rows"):
            AsyncConnection("127.0.0.1:9001", max_total_rows=-1)

    def test_non_int_max_total_rows_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_total_rows"):
            AsyncConnection("127.0.0.1:9001", max_total_rows="oops")  # type: ignore[arg-type]

    def test_negative_max_continuation_frames_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_continuation_frames"):
            AsyncConnection("127.0.0.1:9001", max_continuation_frames=-1)

    def test_non_int_max_continuation_frames_raises_programming_error(self) -> None:
        with pytest.raises(ProgrammingError, match="max_continuation_frames"):
            AsyncConnection("127.0.0.1:9001", max_continuation_frames={})  # type: ignore[arg-type]

    def test_valid_positive_int_accepted(self) -> None:
        c = AsyncConnection("127.0.0.1:9001", max_total_rows=100, max_continuation_frames=50)
        assert c._max_total_rows == 100
        assert c._max_continuation_frames == 50


class TestExceptionChaining:
    """``ProgrammingError`` preserves the original error via ``__cause__``."""

    def test_sync_chains_value_error(self) -> None:
        try:
            Connection("127.0.0.1:9001", max_total_rows=-1)
        except ProgrammingError as e:
            assert isinstance(e.__cause__, ValueError)
        else:
            pytest.fail("expected ProgrammingError")

    def test_sync_chains_type_error(self) -> None:
        try:
            Connection("127.0.0.1:9001", max_total_rows="oops")  # type: ignore[arg-type]
        except ProgrammingError as e:
            assert isinstance(e.__cause__, TypeError)
        else:
            pytest.fail("expected ProgrammingError")
