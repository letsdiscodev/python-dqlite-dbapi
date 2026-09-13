"""The dbapi surface sqlalchemy-dqlite relies on (docs/architecture.md, "Contract with
sqlalchemy-dqlite"). A rename here breaks the dialect, so the names are pinned."""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import aio
from dqlitedbapi.types import format_utc_offset


def test_module_surface() -> None:
    for module in (dqlitedbapi, aio):
        for name in (
            "connect",
            "paramstyle",
            "sqlite_version_info",
            "FAILED_TO_CONNECT_PREFIX",
            "CLUSTER_POLICY_REJECTION_PREFIX",
            "SESSION_MODES",
            "validate_session_mode",
            "Error",
            "InterfaceError",
            "OperationalError",
            "AmbiguousCommitError",
            "DatabaseError",
            "NotSupportedError",
        ):
            assert hasattr(module, name), f"{module.__name__}.{name}"
    assert callable(format_utc_offset)


def test_connection_and_cursor_surface() -> None:
    for connection_cls, cursor_cls in (
        (dqlitedbapi.Connection, dqlitedbapi.Cursor),
        (aio.AsyncConnection, aio.AsyncCursor),
    ):
        for name in (
            "cursor",
            "commit",
            "rollback",
            "close",
            "force_close_transport",
            "connect",
            "session_mode",
            "default_session_mode",
            "set_session_mode",
            "in_transaction",
            "invalidated",
            "closed",
        ):
            assert hasattr(connection_cls, name), f"{connection_cls.__name__}.{name}"
        for name in (
            "execute",
            "executemany",
            "fetchall",
            "description",
            "rowcount",
            "lastrowid",
            "close",
        ):
            assert hasattr(cursor_cls, name), f"{cursor_cls.__name__}.{name}"
