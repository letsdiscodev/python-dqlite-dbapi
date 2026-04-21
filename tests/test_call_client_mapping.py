"""Targeted tests for the exception-type mapping in ``_call_client``.

The generic ``DqliteError`` catch-all is already tested; the two
specific branches below are not, which means a refactor that reorders
or removes them could fall through to the catch-all and silently
change the dbapi exception type surfaced to SQLAlchemy (which keys on
``is_disconnect`` classification).
"""

import asyncio

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import InterfaceError, OperationalError


class TestCallClientClusterErrorMapping:
    def test_cluster_error_becomes_operational_error(self) -> None:
        async def raiser() -> None:
            raise _client_exc.ClusterError("no leader")

        with pytest.raises(OperationalError, match="no leader"):
            asyncio.run(_call_client(raiser()))

    def test_cluster_error_is_not_wrapped_as_interface_error(self) -> None:
        """Regression guard: the catch-all branch would re-classify a
        ClusterError as InterfaceError if the specific branch were
        removed. Assert the mapping is stable so SQLAlchemy's
        is_disconnect path continues to see OperationalError.
        """

        async def raiser() -> None:
            raise _client_exc.ClusterError("no leader")

        with pytest.raises(OperationalError):
            asyncio.run(_call_client(raiser()))

        # Also: it must NOT be an InterfaceError (which would mask the
        # disconnect signal from the dialect's is_disconnect).
        async def raiser2() -> None:
            raise _client_exc.ClusterError("no leader")

        try:
            asyncio.run(_call_client(raiser2()))
        except OperationalError as exc:
            assert not isinstance(exc, InterfaceError)


class TestCallClientInterfaceErrorMapping:
    def test_interface_error_becomes_interface_error(self) -> None:
        async def raiser() -> None:
            raise _client_exc.InterfaceError("Connection is closed")

        with pytest.raises(InterfaceError, match="Connection is closed"):
            asyncio.run(_call_client(raiser()))

    def test_interface_error_is_not_operational_error(self) -> None:
        """The dbapi InterfaceError is a DatabaseError sibling, not a
        subclass of OperationalError. Verify the mapping preserves the
        PEP 249 taxonomy boundary.
        """

        async def raiser() -> None:
            raise _client_exc.InterfaceError("closed")

        try:
            asyncio.run(_call_client(raiser()))
        except Exception as exc:
            assert isinstance(exc, InterfaceError)
            assert not isinstance(exc, OperationalError)


class TestCallClientReturnType:
    """``_call_client`` is a thin exception-mapping wrapper — the
    coroutine's success value must pass through unchanged and with its
    static type preserved so callers that destructure the result (e.g.
    ``last_id, affected = await _call_client(conn.execute(...))``) keep
    compile-time checks on the shape.
    """

    def test_returns_value_unchanged(self) -> None:
        async def produce_tuple() -> tuple[int, int]:
            return 7, 3

        result = asyncio.run(_call_client(produce_tuple()))
        assert result == (7, 3)

    def test_return_type_narrows_to_coroutine_type(self) -> None:
        """Pin the TypeVar narrowing: ``_call_client(coro)`` must
        declare the same return type as ``coro`` so type-checkers can
        verify tuple destructures and attribute access at call-sites.
        A regression widening the signature back to ``Any`` silently
        erases this guarantee and would not fail a runtime test,
        so we use ``typing.assert_type`` (evaluated at type-check
        time; a no-op at runtime that still documents the contract).
        """
        from typing import assert_type

        async def produce_int() -> int:
            return 42

        async def probe() -> None:
            v = await _call_client(produce_int())
            assert_type(v, int)
            assert v == 42

        asyncio.run(probe())
