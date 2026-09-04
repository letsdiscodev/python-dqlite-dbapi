"""Tests for the specific exception-type mapping branches in ``_call_client`` that, if
removed, would fall through to the catch-all and change the type SQLAlchemy keys on."""

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
        """ClusterError must stay OperationalError (not InterfaceError) for SA's is_disconnect."""

        async def raiser() -> None:
            raise _client_exc.ClusterError("no leader")

        with pytest.raises(OperationalError) as exc_info:
            asyncio.run(_call_client(raiser()))
        assert not isinstance(exc_info.value, InterfaceError)

    def test_cluster_error_wraps_with_code_none(self) -> None:
        """ClusterError carries no SQLite code; pin ``.code is None`` for sibling-arm parity."""

        async def raiser() -> None:
            raise _client_exc.ClusterError("no leader")

        with pytest.raises(OperationalError) as exc_info:
            asyncio.run(_call_client(raiser()))

        assert exc_info.value.code is None
        assert isinstance(exc_info.value.__cause__, _client_exc.ClusterError)


class TestCallClientInterfaceErrorMapping:
    def test_interface_error_becomes_interface_error(self) -> None:
        async def raiser() -> None:
            raise _client_exc.InterfaceError("Connection is closed")

        with pytest.raises(InterfaceError, match="Connection is closed"):
            asyncio.run(_call_client(raiser()))

    def test_interface_error_is_not_operational_error(self) -> None:
        """InterfaceError is a DatabaseError sibling, not an OperationalError subclass."""

        async def raiser() -> None:
            raise _client_exc.InterfaceError("closed")

        with pytest.raises(InterfaceError) as exc_info:
            asyncio.run(_call_client(raiser()))
        assert not isinstance(exc_info.value, OperationalError)


class TestCallClientClusterPolicyErrorMapping:
    """ClusterPolicyError → InterfaceError with a "Cluster policy rejection;" prefix; SA's
    is_disconnect narrows matching so the pool invalidates without retrying the policy wall."""

    def test_becomes_interface_error(self) -> None:
        async def raiser() -> None:
            raise _client_exc.ClusterPolicyError("leader not in allow-list")

        with pytest.raises(InterfaceError) as exc_info:
            asyncio.run(_call_client(raiser()))
        assert "Cluster policy rejection;" in str(exc_info.value)
        assert "leader not in allow-list" in str(exc_info.value)

    def test_chains_original_via_cause(self) -> None:
        async def raiser() -> None:
            raise _client_exc.ClusterPolicyError("policy")

        with pytest.raises(InterfaceError) as exc_info:
            asyncio.run(_call_client(raiser()))
        assert isinstance(exc_info.value.__cause__, _client_exc.ClusterPolicyError)

    def test_is_not_operational_error(self) -> None:
        """Must not be OperationalError, else SA would retry against a permanent rejection."""

        async def raiser() -> None:
            raise _client_exc.ClusterPolicyError("policy")

        with pytest.raises(InterfaceError) as exc_info:
            asyncio.run(_call_client(raiser()))
        assert not isinstance(exc_info.value, OperationalError)


class TestCallClientReturnType:
    """``_call_client`` passes the coroutine's success value through unchanged."""

    def test_returns_value_unchanged(self) -> None:
        async def produce_tuple() -> tuple[int, int]:
            return 7, 3

        result = asyncio.run(_call_client(produce_tuple()))
        assert result == (7, 3)

    # TypeVar narrowing of _call_client(coro) -> T is verified statically by mypy, not at runtime.
