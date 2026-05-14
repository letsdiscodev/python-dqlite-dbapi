"""Pin: ``iterdump``, ``enable_load_extension``, and
``load_extension`` stubs accept any call signature and route every
misuse through the ``dqlitedbapi.Error`` hierarchy.

Every un-implementable stdlib stub uses
``def stub(self, *args: object, **kwargs: object) -> NoReturn`` so
any caller signature — positional, keyword, novel-stdlib-3.13
``filter=`` kwarg — reaches ``_stub_unsupported`` and surfaces a
``NotSupportedError`` inside the dbapi exception hierarchy.

The three subjects had previously carried tightly-typed signatures
that leaked bare ``TypeError`` outside ``dqlitedbapi.Error`` for any
signature mismatch — breaking cross-driver feature-probe code. This
pin forecloses a re-tightening regression.
"""

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
        ("iterdump", (), {"filter": "*", "novel_kwarg": True}),  # **kwargs absorbs unknown
        ("enable_load_extension", (), {}),  # zero-arg
        ("enable_load_extension", (True,), {}),
        ("enable_load_extension", (), {"enabled": True}),
        ("enable_load_extension", (True,), {"extra": "ignored"}),
        ("load_extension", (), {"path": "x.so"}),  # kwarg form
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
    """The headline regression: a Python-3.13 caller doing
    ``conn.iterdump(filter="x")`` must NOT see a bare ``TypeError``
    from the signature mismatch — that escapes ``except dbapi.Error``
    and breaks cross-driver feature-probe code."""
    with pytest.raises(dqlitedbapi.NotSupportedError):
        sync_conn.iterdump(filter="x")


def test_enable_load_extension_zero_arg_does_not_leak_typeerror(
    sync_conn: dqlitedbapi.Connection,
) -> None:
    """A caller writing ``conn.enable_load_extension()`` (expecting
    'default off') previously saw a bare ``TypeError`` from the
    missing positional argument. The stub must absorb the call."""
    with pytest.raises(dqlitedbapi.NotSupportedError):
        sync_conn.enable_load_extension()


# --- TPC stubs (PEP 249 §7) -----------------------------------------------
# Mirror the iterdump / load_extension / enable_load_extension matrix for
# the six TPC stubs (tpc_begin / tpc_prepare / tpc_commit / tpc_rollback /
# tpc_recover / xid). dqlite cannot support TPC (Raft is a single-cluster
# commit log, no XA coordinator), so any caller signature must surface
# NotSupportedError inside the dbapi.Error hierarchy.


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("tpc_begin", (), {}),  # missing positional in old signature
        ("tpc_begin", (object(), object()), {}),  # too many positionals
        ("tpc_begin", (object(),), {"format": 1}),  # novel kwarg
        ("tpc_prepare", (object(),), {}),  # extra positional
        ("tpc_commit", (object(), object()), {}),  # too many positionals
        ("tpc_commit", (), {"novel": True}),  # novel kwarg
        ("tpc_rollback", (), {"novel": True}),  # novel kwarg
        ("tpc_recover", (), {"timeout": 5}),  # novel kwarg
        ("xid", (), {}),  # missing 3 positionals
        ("xid", (1, "g"), {}),  # missing 1 positional
        ("xid", (1, "g", "b", "extra"), {}),  # too many positionals
        ("xid", (1, "g", "b"), {"novel": True}),  # novel kwarg
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
