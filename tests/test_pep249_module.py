"""PEP 249 module surface: globals, exports, aliases, stubs, constants, and import side effects."""

from __future__ import annotations

import inspect
import logging
import os
import pathlib
import pkgutil
import sqlite3
import subprocess
import sys
import textwrap
import tomllib
from collections.abc import Iterator
from pathlib import Path

import pytest

import dqlitedbapi  # noqa: F401 -- import for side effect
import dqlitedbapi.aio  # noqa: F401 -- async surface must propagate too
import dqlitedbapi.exceptions as exc_mod
from dqliteclient import DEFAULT_CLOSE_TIMEOUT_SECONDS, DEFAULT_TIMEOUT_SECONDS
from dqlitedbapi import Connection, ProgrammingError, aio
from dqlitedbapi import connect as sync_connect
from dqlitedbapi import types as _types
from dqlitedbapi import types as dqlite_types
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio import connect as aio_connect
from dqlitedbapi.exceptions import NotSupportedError
from dqlitedbapi.types import DescriptionTuple


def test_pyproject_matches_package_version() -> None:
    pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
    with pyproject.open("rb") as f:
        metadata = tomllib.load(f)
    assert metadata["project"]["version"] == dqlitedbapi.__version__


class TestVersionExport:
    def test_version_in_all(self) -> None:
        assert "__version__" in dqlitedbapi.__all__

    def test_version_is_string(self) -> None:
        assert isinstance(dqlitedbapi.__version__, str)

    def test_aio_module_exports_version(self) -> None:
        assert hasattr(aio, "__version__")
        assert aio.__version__ == dqlitedbapi.__version__

    def test_aio_version_in_all(self) -> None:
        assert "__version__" in aio.__all__


@pytest.fixture
def cur() -> Iterator[dqlitedbapi.Cursor]:
    conn = dqlitedbapi.connect("localhost:9001", timeout=2.0)
    cursor = conn.cursor()
    yield cursor
    conn.close()


def test_named_param_sql_with_dict_rejected_with_mapping_diagnostic(
    cur: dqlitedbapi.Cursor,
) -> None:
    """Mappings are rejected up front, before any wire round-trip."""
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT :name", {"name": "x"})  # type: ignore[arg-type]


def test_named_param_sql_with_list_falls_through_to_bind_count(
    cur: dqlitedbapi.Cursor,
) -> None:
    """``:name`` SQL (0 placeholders) with a 1-element sequence hits bind-count."""
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
        cur.execute("SELECT :name", ["x"])


def test_pyformat_sql_with_tuple_falls_through_to_bind_count(
    cur: dqlitedbapi.Cursor,
) -> None:
    """``%s`` (psycopg-style) SQL with a 1-tuple hits bind-count rejection."""
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
        cur.execute("SELECT %s", ("x",))


_PEP249_ALIAS_NAMES = (
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    # dqlite-specific OperationalError subclass, mirrored like the mandated names.
    "AmbiguousCommitError",
)


@pytest.mark.parametrize("name", _PEP249_ALIAS_NAMES)
def test_sync_connection_alias_identity(name: str) -> None:
    alias = getattr(Connection, name)
    module_class = getattr(dqlitedbapi, name)
    assert alias is module_class, (
        f"Connection.{name} should be the same class object as dqlitedbapi.{name}"
    )


@pytest.mark.parametrize("name", _PEP249_ALIAS_NAMES)
def test_async_connection_alias_identity(name: str) -> None:
    alias = getattr(AsyncConnection, name)
    module_class = getattr(dqlitedbapi.aio, name)
    assert alias is module_class, (
        f"AsyncConnection.{name} should be the same class object as dqlitedbapi.aio.{name}"
    )


def test_operational_error_via_connection_alias_preserves_code() -> None:
    """The alias must be the SAME class, not a subclass, so custom code= kwarg works."""
    with pytest.raises(Connection.OperationalError) as excinfo:
        raise Connection.OperationalError("explode", code=42)
    assert excinfo.value.code == 42
    assert str(excinfo.value) == "explode"


def test_catch_via_instance_attribute() -> None:
    """PEP 249 covers both Class.Error and instance.Error."""
    conn = Connection("localhost:9001")
    try:
        raise dqlitedbapi.DataError("boom")
    except conn.Error as exc:
        assert isinstance(exc, dqlitedbapi.Error)


def test_async_catch_via_instance_attribute() -> None:
    aconn = AsyncConnection("localhost:9001")
    try:
        raise dqlitedbapi.DataError("boom")
    except aconn.Error as exc:
        assert isinstance(exc, dqlitedbapi.Error)


def test_exceptions_all_includes_every_public_error_class() -> None:
    public_classes = {
        name
        for name in dir(exc_mod)
        if not name.startswith("_")
        and isinstance(getattr(exc_mod, name), type)
        and issubclass(getattr(exc_mod, name), (exc_mod.Error, exc_mod.Warning))
    }
    missing = public_classes - set(exc_mod.__all__)
    assert not missing, (
        f"dqlitedbapi.exceptions.__all__ is missing public Error/Warning "
        f"subclasses: {sorted(missing)}"
    )


def test_ambiguous_commit_error_in_exceptions_all() -> None:
    assert "AmbiguousCommitError" in exc_mod.__all__


def test_wildcard_import_from_exceptions_includes_ambiguous_commit_error() -> None:
    """Wildcard import from ``dqlitedbapi.exceptions`` brings the class into scope."""
    ns: dict[str, object] = {}
    exec("from dqlitedbapi.exceptions import *", ns)
    assert "AmbiguousCommitError" in ns


def test_dbapi_does_not_shadow_wire_leader_error_codes() -> None:
    from dqlitewire import LEADER_ERROR_CODES as wire_codes

    for module_info in pkgutil.walk_packages(
        dqlitedbapi.__path__,
        prefix=f"{dqlitedbapi.__name__}.",
    ):
        module = __import__(module_info.name, fromlist=["_"])
        local = getattr(module, "LEADER_ERROR_CODES", None)
        if local is None:
            continue
        # A re-import here must preserve identity (same wire object), not a copy.
        assert local is wire_codes, (
            f"{module_info.name} defines LEADER_ERROR_CODES as a local "
            f"copy (not the dqlitewire SSOT). Import the wire constant "
            f"directly: ``from dqlitewire import LEADER_ERROR_CODES``."
        )


@pytest.fixture
def conn() -> Connection:
    return Connection("localhost:9001", timeout=1.0)


_STUB_NAMES = [
    "tpc_begin",
    "tpc_prepare",
    "tpc_commit",
    "tpc_rollback",
    "tpc_recover",
    "xid",
    "enable_load_extension",
    "load_extension",
    "backup",
    "iterdump",
    "create_function",
    "create_aggregate",
    "create_collation",
    "create_window_function",
    # total_changes is a method, not a @property, to keep the hasattr-True invariant.
    "total_changes",
]


@pytest.mark.parametrize("name", _STUB_NAMES)
def test_connection_stub_methods_present_for_pep249_compliance(conn: Connection, name: str) -> None:
    assert hasattr(conn, name)
    method = getattr(conn, name)
    assert callable(method)


@pytest.mark.parametrize("name", _STUB_NAMES)
def test_async_connection_stub_methods_present_for_pep249_compliance(name: str) -> None:
    from dqlitedbapi.aio.connection import AsyncConnection

    aconn = AsyncConnection("localhost:9001", timeout=1.0)
    assert hasattr(aconn, name)
    method = getattr(aconn, name)
    assert callable(method)


def test_connection_tpc_methods_raise_not_supported(conn: Connection) -> None:
    """Stubs raise NotSupportedError (a dbapi.Error subclass)."""
    with pytest.raises(NotSupportedError, match="two-phase commit"):
        conn.tpc_begin(object())


def test_cursor_callproc_nextset_scroll_present_but_raise() -> None:
    """Cursor stubs are present (hasattr True) but raise NotSupportedError."""
    conn = Connection("localhost:9001", timeout=1.0)
    cur = conn.cursor()
    try:
        for name in ("callproc", "nextset", "scroll"):
            assert hasattr(cur, name)

        with pytest.raises(NotSupportedError, match="stored procedures"):
            cur.callproc("foo")
        with pytest.raises(NotSupportedError, match="multiple result sets"):
            cur.nextset()
        with pytest.raises(NotSupportedError, match="not scrollable"):
            cur.scroll(0)
    finally:
        cur.close()
        conn.close()


def test_hasattr_total_changes_sync_does_not_raise() -> None:
    conn = dqlitedbapi.Connection("localhost:9001", timeout=1.0)
    try:
        try:
            present = hasattr(conn, "total_changes")
        except NotSupportedError:
            pytest.fail(
                "hasattr() leaked NotSupportedError — total_changes must be a "
                "method-stub like the rest of the family, not a @property"
            )
        assert present is True
        # And calling the stub still raises (parens — method form).
        with pytest.raises(NotSupportedError, match="total_changes"):
            conn.total_changes()
    finally:
        conn.close()


def test_hasattr_total_changes_async_does_not_raise() -> None:
    """Async sibling: AsyncConnection's ``total_changes`` was also a ``@property`` outlier."""
    aconn = AsyncConnection("localhost:9001", timeout=1.0)
    try:
        present = hasattr(aconn, "total_changes")
    except NotSupportedError:
        pytest.fail(
            "hasattr() leaked NotSupportedError on AsyncConnection — "
            "total_changes must be a method-stub like the rest of the family"
        )
    assert present is True
    with pytest.raises(NotSupportedError, match="total_changes"):
        aconn.total_changes()


def test_prepareprotocol_in_types_all_pin() -> None:
    assert "PrepareProtocol" in _types.__all__


def test_prepareprotocol_top_level_import_pin() -> None:
    assert hasattr(dqlitedbapi, "PrepareProtocol")


def test_prepareprotocol_stdlib_parity_pin() -> None:
    """PrepareProtocol is a class, mirroring stdlib ``sqlite3.PrepareProtocol``."""
    assert isinstance(dqlitedbapi.PrepareProtocol, type)


def test_prepareprotocol_top_level_and_types_module_identity_match() -> None:
    """Same class object behind both the top-level and ``types`` re-exports."""
    assert dqlitedbapi.PrepareProtocol is _types.PrepareProtocol


def test_description_tuple_is_publicly_exported_from_types_module() -> None:
    assert hasattr(dqlite_types, "DescriptionTuple")


def test_description_tuple_is_in_types_module_all() -> None:
    assert "DescriptionTuple" in dqlite_types.__all__


def test_description_tuple_is_publicly_exported_from_package_root() -> None:
    assert hasattr(dqlitedbapi, "DescriptionTuple")
    assert dqlitedbapi.DescriptionTuple is dqlite_types.DescriptionTuple
    assert "DescriptionTuple" in dqlitedbapi.__all__


def test_description_tuple_is_pep_695_type_alias() -> None:
    """Public type aliases use PEP 695 ``type X = ...`` (a ``TypeAliasType``)."""
    import typing

    assert isinstance(dqlite_types.DescriptionTuple, typing.TypeAliasType), (
        "DescriptionTuple must be declared as 'type DescriptionTuple = ...' "
        "to match the workspace's PEP 695 discipline"
    )


def test_async_surface_exports_description_tuple_in_all() -> None:
    assert "DescriptionTuple" in dqlitedbapi.aio.__all__


def test_async_surface_description_tuple_resolves_to_canonical() -> None:
    assert dqlitedbapi.aio.DescriptionTuple is DescriptionTuple
    assert dqlitedbapi.aio.DescriptionTuple is dqlitedbapi.DescriptionTuple


def test_sqlite_version_uppercase_not_public_on_dbapi() -> None:
    import dqlitedbapi

    assert not hasattr(dqlitedbapi, "SQLITE_VERSION"), (
        "SQLITE_VERSION must be private (use sqlite_version)"
    )
    assert not hasattr(dqlitedbapi, "SQLITE_VERSION_INFO"), (
        "SQLITE_VERSION_INFO must be private (use sqlite_version_info)"
    )


def test_sqlite_version_uppercase_not_public_on_aio() -> None:
    from dqlitedbapi import aio as aio_mod

    assert not hasattr(aio_mod, "SQLITE_VERSION"), (
        "SQLITE_VERSION must be private (use sqlite_version)"
    )
    assert not hasattr(aio_mod, "SQLITE_VERSION_INFO"), (
        "SQLITE_VERSION_INFO must be private (use sqlite_version_info)"
    )


def test_lowercase_public_form_remains() -> None:
    import dqlitedbapi
    from dqlitedbapi import aio as aio_mod

    assert isinstance(dqlitedbapi.sqlite_version, str)
    assert isinstance(dqlitedbapi.sqlite_version_info, tuple)
    assert isinstance(aio_mod.sqlite_version, str)
    assert isinstance(aio_mod.sqlite_version_info, tuple)
    assert dqlitedbapi.sqlite_version == aio_mod.sqlite_version
    assert dqlitedbapi.sqlite_version_info == aio_mod.sqlite_version_info


def test_sync_module_exposes_legacy_transaction_control() -> None:
    assert dqlitedbapi.LEGACY_TRANSACTION_CONTROL == -1
    assert dqlitedbapi.LEGACY_TRANSACTION_CONTROL == sqlite3.LEGACY_TRANSACTION_CONTROL


def test_async_module_exposes_legacy_transaction_control() -> None:
    assert dqlitedbapi.aio.LEGACY_TRANSACTION_CONTROL == -1
    assert dqlitedbapi.aio.LEGACY_TRANSACTION_CONTROL == sqlite3.LEGACY_TRANSACTION_CONTROL


def test_legacy_transaction_control_in_sync_all() -> None:
    assert "LEGACY_TRANSACTION_CONTROL" in dqlitedbapi.__all__


def test_legacy_transaction_control_in_async_all() -> None:
    assert "LEGACY_TRANSACTION_CONTROL" in dqlitedbapi.aio.__all__


def test_legacy_transaction_control_setter_round_trip() -> None:
    """The exposed constant is the value the autocommit setter accepts."""
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = dqlitedbapi.LEGACY_TRANSACTION_CONTROL
    finally:
        conn.close()


def test_sync_module_exposes_parse_decltypes() -> None:
    assert dqlitedbapi.PARSE_DECLTYPES == 1
    assert dqlitedbapi.PARSE_DECLTYPES == sqlite3.PARSE_DECLTYPES


def test_sync_module_exposes_parse_colnames() -> None:
    assert dqlitedbapi.PARSE_COLNAMES == 2
    assert dqlitedbapi.PARSE_COLNAMES == sqlite3.PARSE_COLNAMES


def test_async_module_exposes_parse_decltypes() -> None:
    assert dqlitedbapi.aio.PARSE_DECLTYPES == 1


def test_async_module_exposes_parse_colnames() -> None:
    assert dqlitedbapi.aio.PARSE_COLNAMES == 2


def test_parse_constants_in_sync_all() -> None:
    assert "PARSE_DECLTYPES" in dqlitedbapi.__all__
    assert "PARSE_COLNAMES" in dqlitedbapi.__all__


def test_parse_constants_in_async_all() -> None:
    assert "PARSE_DECLTYPES" in dqlitedbapi.aio.__all__
    assert "PARSE_COLNAMES" in dqlitedbapi.aio.__all__


def test_detect_types_still_rejected_at_connect_kwarg_gate() -> None:
    with pytest.raises(NotSupportedError, match="detect_types"):
        dqlitedbapi.connect(
            "127.0.0.1:9999",
            detect_types=dqlitedbapi.PARSE_DECLTYPES,
        )


def test_detect_types_still_rejected_at_aconnect_kwarg_gate() -> None:
    with pytest.raises(NotSupportedError, match="detect_types"):
        dqlitedbapi.aio.connect(
            "127.0.0.1:9999",
            detect_types=dqlitedbapi.aio.PARSE_COLNAMES,
        )


def test_sync_connect_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(sync_connect).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS


def test_aio_connect_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(aio_connect).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS


def test_sync_connection_class_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(Connection.__init__).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS


def test_async_connection_class_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(AsyncConnection.__init__).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS


def test_top_level_logger_has_null_handler() -> None:
    logger = logging.getLogger("dqlitedbapi")
    assert any(isinstance(h, logging.NullHandler) for h in logger.handlers), (
        "library top-level logger must have a NullHandler attached per "
        "Python logging HOWTO convention"
    )


def test_aio_sub_package_inherits_via_propagation() -> None:
    """The aio sub-package logger relies on parent-propagation rather
    than its own handler. Verify propagation is on (the default)."""
    logger = logging.getLogger("dqlitedbapi.aio")
    assert logger.propagate is True, (
        "aio sub-package logger must propagate to the parent so the "
        "parent's NullHandler catches its records"
    )


def _run_subprocess_and_assert_wire_loaded(import_target: str) -> None:
    repo_src = Path(__file__).resolve().parent.parent / "src"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(repo_src)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    )
    snippet = f"""
        import sys
        assert "dqlitewire" not in sys.modules
        import {import_target}  # noqa: F401
        if "dqlitewire" not in sys.modules:
            print("FAIL: {import_target} did not transitively load dqlitewire", flush=True)
            sys.exit(1)
        print("OK", flush=True)
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(snippet)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"transitive-wire-import pin failed for {import_target}:\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    assert "OK" in result.stdout


def test_dqlitewire_loaded_after_dqlitedbapi_import() -> None:
    _run_subprocess_and_assert_wire_loaded("dqlitedbapi")


def test_dqlitewire_loaded_after_dqlitedbapi_aio_import() -> None:
    _run_subprocess_and_assert_wire_loaded("dqlitedbapi.aio")
