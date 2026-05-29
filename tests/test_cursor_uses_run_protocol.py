"""Cursors must route through DqliteConnection's public API (conn.execute() / query_raw())
rather than touching conn._protocol directly, so they get the _in_use guard, fatal-error
invalidation, and leader-change detection from _run_protocol().
"""

import ast
import inspect
import textwrap
from typing import Any

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _has_direct_attr_access(func: Any, attr_name: str) -> bool:
    """True if ``func`` accesses ``conn.<attr_name>`` directly (via AST)."""
    source = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr == attr_name
            and isinstance(node.value, ast.Name)
            and node.value.id == "conn"
        ):
            return True
    return False


class TestSyncCursorDoesNotAccessProtocolDirectly:
    def test_execute_async_does_not_access_conn_protocol(self) -> None:
        assert not _has_direct_attr_access(Cursor._execute_async, "_protocol"), (
            "Cursor._execute_async accesses conn._protocol directly. "
            "It should use conn.execute() / conn.query_raw()."
        )

    def test_execute_async_does_not_access_conn_db_id(self) -> None:
        assert not _has_direct_attr_access(Cursor._execute_async, "_db_id"), (
            "Cursor._execute_async accesses conn._db_id directly. "
            "It should use the public API on DqliteConnection."
        )


class TestAsyncCursorDoesNotAccessProtocolDirectly:
    def test_execute_does_not_access_conn_protocol(self) -> None:
        assert not _has_direct_attr_access(AsyncCursor.execute, "_protocol"), (
            "AsyncCursor.execute accesses conn._protocol directly. "
            "It should use conn.execute() / conn.query_raw()."
        )

    def test_execute_does_not_access_conn_db_id(self) -> None:
        assert not _has_direct_attr_access(AsyncCursor.execute, "_db_id"), (
            "AsyncCursor.execute accesses conn._db_id directly. "
            "It should use the public API on DqliteConnection."
        )
