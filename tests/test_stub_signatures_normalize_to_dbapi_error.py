"""iterdump/enable_load_extension/load_extension stubs surface NotSupportedError (inside
dqlitedbapi.Error) instead of leaking a bare TypeError on keyword mismatch."""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


@pytest.fixture
def sync_conn() -> dqlitedbapi.Connection:
    return dqlitedbapi.Connection("localhost:9001")


@pytest.fixture
def async_conn() -> AsyncConnection:
    return AsyncConnection("localhost:9001")


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("iterdump", (), {}),
        ("iterdump", (), {"filter": "*"}),  # stdlib 3.13 added kwarg
        ("iterdump", (), {"filter": "*", "novel_kwarg": True}),
        ("enable_load_extension", (), {}),
        ("enable_load_extension", (True,), {}),
        ("enable_load_extension", (), {"enabled": True}),
        ("enable_load_extension", (True,), {"extra": "ignored"}),
        ("load_extension", (), {"path": "x.so"}),
        ("load_extension", ("x.so",), {}),
        ("load_extension", ("x.so",), {"entrypoint": "init", "extra": 1}),
    ],
)
def test_sync_stub_routes_through_notsupported_error(
    sync_conn: dqlitedbapi.Connection,
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        getattr(sync_conn, method)(*args, **kwargs)


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("iterdump", (), {}),
        ("iterdump", (), {"filter": "*"}),
        ("enable_load_extension", (), {}),
        ("enable_load_extension", (True,), {}),
        ("load_extension", (), {"path": "x.so"}),
        ("load_extension", ("x.so",), {"entrypoint": "init"}),
    ],
)
def test_async_stub_routes_through_notsupported_error(
    async_conn: AsyncConnection,
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        getattr(async_conn, method)(*args, **kwargs)


def test_iterdump_filter_kwarg_does_not_leak_typeerror(
    sync_conn: dqlitedbapi.Connection,
) -> None:
    """iterdump(filter="x") from a 3.13 caller must not leak a bare TypeError."""
    with pytest.raises(dqlitedbapi.NotSupportedError):
        sync_conn.iterdump(filter="x")


def test_enable_load_extension_zero_arg_does_not_leak_typeerror(
    sync_conn: dqlitedbapi.Connection,
) -> None:
    """enable_load_extension() with no args must absorb the call, not leak a TypeError."""
    with pytest.raises(dqlitedbapi.NotSupportedError):
        sync_conn.enable_load_extension()


# dqlite cannot support TPC (Raft is a single-cluster log, no XA coordinator), so the six
# TPC stubs surface NotSupportedError for any signature.


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("tpc_begin", (), {}),
        ("tpc_begin", (object(), object()), {}),
        ("tpc_begin", (object(),), {"format": 1}),
        ("tpc_prepare", (object(),), {}),
        ("tpc_commit", (object(), object()), {}),
        ("tpc_commit", (), {"novel": True}),
        ("tpc_rollback", (), {"novel": True}),
        ("tpc_recover", (), {"timeout": 5}),
        ("xid", (), {}),
        ("xid", (1, "g"), {}),
        ("xid", (1, "g", "b", "extra"), {}),
        ("xid", (1, "g", "b"), {"novel": True}),
    ],
)
def test_sync_tpc_stub_routes_through_notsupported_error(
    sync_conn: dqlitedbapi.Connection,
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        getattr(sync_conn, method)(*args, **kwargs)


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("tpc_begin", (), {}),
        ("tpc_begin", (object(),), {"format": 1}),
        ("tpc_prepare", (object(),), {}),
        ("tpc_commit", (), {"novel": True}),
        ("tpc_rollback", (), {"novel": True}),
        ("tpc_recover", (), {"timeout": 5}),
        ("xid", (), {}),
        ("xid", (1, "g", "b"), {"novel": True}),
    ],
)
def test_async_tpc_stub_routes_through_notsupported_error(
    async_conn: AsyncConnection,
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        getattr(async_conn, method)(*args, **kwargs)
