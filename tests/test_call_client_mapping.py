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

        # Also: it must NOT be an InterfaceError (which would mask the
        # disconnect signal from the dialect's is_disconnect). Chain
        # the negative assertion off ``exc_info.value`` so a regression
        # where ``_call_client`` returns normally fails the
        # ``pytest.raises`` upfront — earlier shape used a bare
        # ``try/except`` which would silently pass on no-raise.
        with pytest.raises(OperationalError) as exc_info:
            asyncio.run(_call_client(raiser()))
        assert not isinstance(exc_info.value, InterfaceError)

    def test_cluster_error_wraps_with_code_none(self) -> None:
        """Signature parity with the sibling DqliteConnectionError arm
        (which explicitly passes ``code=None``). ClusterError carries
        no SQLite code today; pinning ``.code is None`` catches a
        future refactor that accidentally forwards a bogus code or
        drops the explicit kwarg on the sibling in a way that makes
        the two arms diverge again.
        """

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
        """The dbapi InterfaceError is a DatabaseError sibling, not a
        subclass of OperationalError. Verify the mapping preserves the
        PEP 249 taxonomy boundary.

        Use ``pytest.raises`` so a regression where ``_call_client``
        silently swallows ``client.InterfaceError`` (returning ``None``
        instead of re-raising) is caught — a bare ``try/except``
        without an enforced raise would silently pass on the
        no-exception branch.
        """

        async def raiser() -> None:
            raise _client_exc.InterfaceError("closed")

        with pytest.raises(InterfaceError) as exc_info:
            asyncio.run(_call_client(raiser()))
        assert not isinstance(exc_info.value, OperationalError)


class TestCallClientClusterPolicyErrorMapping:
    """``ClusterPolicyError`` routes to ``InterfaceError`` with a
    distinguishing ``"Cluster policy rejection;"`` prefix. The SA
    dialect's ``is_disconnect`` narrows ``InterfaceError`` matching to
    "connection is closed" / "cursor is closed" so the pool invalidates
    the permanent-reject slot without scheduling a retry against the
    policy wall.
    """

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
        """Must not land in ``OperationalError`` — routing there would
        let SA's substring list match "Failed to connect" variants and
        re-enter the retry loop against a permanent rejection."""

        async def raiser() -> None:
            raise _client_exc.ClusterPolicyError("policy")

        with pytest.raises(InterfaceError) as exc_info:
            asyncio.run(_call_client(raiser()))
        assert not isinstance(exc_info.value, OperationalError)


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

    # Note: TypeVar narrowing of ``_call_client(coro)`` is verified
    # statically by mypy on the project's type-checking pass — the
    # signature ``_call_client(coro: Coroutine[Any, Any, T]) -> T``
    # is a static contract. A runtime test using ``typing.assert_type``
    # was previously included here; ``assert_type`` is a no-op at
    # runtime, so the runtime portion was redundant with
    # ``test_returns_value_unchanged`` above. The static contract
    # remains pinned by mypy's coverage of the dbapi sources.
