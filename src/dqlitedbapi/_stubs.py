"""``NotSupportedError`` stubs for stdlib ``sqlite3`` APIs with no dqlite counterpart."""

from typing import NoReturn

from dqlitedbapi.exceptions import NotSupportedError

_TWO_PHASE = "dqlite does not support two-phase commit"
_NO_CALLBACKS = "dqlite-server does not expose SQLite callbacks over the wire"
_NO_EXTENSIONS = "dqlite-server does not support runtime extension loading"
_NO_UDF = "dqlite-server does not support user-defined SQL functions"
_NO_DB_HANDLE = "dqlite does not expose the SQLite database handle over the wire"
_NO_DUMP = "dqlite has no client-side dump/backup API; use the server's dump mechanism"


class UnsupportedSqlite3Api:
    """Mixin giving connections the stdlib method names, each raising ``NotSupportedError``."""

    def _unsupported(self, name: str, reason: str) -> NoReturn:
        raise NotSupportedError(f"{name} is not supported: {reason}")

    def executescript(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported(
            "executescript()",
            "there is no multi-statement primitive; execute one statement at a time",
        )

    def interrupt(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("interrupt()", "use asyncio.timeout() or the per-RPC timeout instead")

    def tpc_begin(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("tpc_begin()", _TWO_PHASE)

    def tpc_prepare(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("tpc_prepare()", _TWO_PHASE)

    def tpc_commit(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("tpc_commit()", _TWO_PHASE)

    def tpc_rollback(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("tpc_rollback()", _TWO_PHASE)

    def tpc_recover(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("tpc_recover()", _TWO_PHASE)

    def xid(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("xid()", _TWO_PHASE)

    def set_authorizer(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("set_authorizer()", _NO_CALLBACKS)

    def set_progress_handler(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("set_progress_handler()", _NO_CALLBACKS)

    def set_trace_callback(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("set_trace_callback()", _NO_CALLBACKS)

    def total_changes(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("total_changes", _NO_DB_HANDLE)

    def getlimit(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("getlimit()", _NO_DB_HANDLE)

    def setlimit(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("setlimit()", _NO_DB_HANDLE)

    def getconfig(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("getconfig()", _NO_DB_HANDLE)

    def setconfig(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("setconfig()", _NO_DB_HANDLE)

    def serialize(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("serialize()", _NO_DB_HANDLE)

    def deserialize(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("deserialize()", _NO_DB_HANDLE)

    def blobopen(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("blobopen()", _NO_DB_HANDLE)

    def enable_load_extension(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("enable_load_extension()", _NO_EXTENSIONS)

    def load_extension(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("load_extension()", _NO_EXTENSIONS)

    def backup(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("backup()", _NO_DUMP)

    def iterdump(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("iterdump()", _NO_DUMP)

    def create_function(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("create_function()", _NO_UDF)

    def create_aggregate(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("create_aggregate()", _NO_UDF)

    def create_collation(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("create_collation()", _NO_UDF)

    def create_window_function(self, *args: object, **kwargs: object) -> NoReturn:
        self._unsupported("create_window_function()", _NO_UDF)
