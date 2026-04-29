"""``Connection.__init__`` and ``AsyncConnection.__init__`` must wrap
the client-layer ``_validate_positive_int_or_none``'s
``TypeError``/``ValueError`` into PEP 249 ``ProgrammingError``.

The client-layer validator deliberately raises Python-convention
exceptions (per the original ISSUE-39 contract for client-only
consumers). The dbapi entry points are the PEP 249 boundary: every
error originating from the driver must be a subclass of
``dqlitedbapi.Error`` (PEP 249 §7). The sibling validator
``_validate_timeout`` already wraps to ``ProgrammingError``; the
parallel ``_client_parse_address`` ``ValueError`` is wrapped to
``InterfaceError``. ``_validate_positive_int_or_none`` was the
remaining outlier — passing through raw ``TypeError``/``ValueError``
for ``max_total_rows`` / ``max_continuation_frames``.
"""

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
        """Sanity: well-formed positive ints still construct."""
        c = Connection("127.0.0.1:9001", max_total_rows=100, max_continuation_frames=50)
        assert c._max_total_rows == 100
        assert c._max_continuation_frames == 50

    def test_none_accepted_for_both(self) -> None:
        """``None`` means "no cap" — must remain accepted."""
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
    """``ProgrammingError`` must preserve the original
    ``TypeError`` / ``ValueError`` via ``__cause__`` so callers
    debugging from the dbapi error can still see the underlying
    Python-convention message."""

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
