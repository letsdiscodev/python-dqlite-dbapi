"""Sync cursor: a thin adapter over :class:`dqlitedbapi.aio.AsyncCursor`."""

from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, NoReturn, Self

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.types import Description, RowFactory

if TYPE_CHECKING:
    from dqlitedbapi.connection import Connection

__all__ = ["Cursor"]


class Cursor:
    """PEP 249 cursor. Results are fully buffered at execute time."""

    __slots__ = ("_async", "_connection", "messages")

    def __init__(self, connection: "Connection", async_cursor: AsyncCursor) -> None:
        self._connection = connection
        self._async = async_cursor
        self.messages: list[tuple[type[Exception], Exception]] = []

    @property
    def connection(self) -> "Connection":
        return self._connection

    @property
    def description(self) -> Description:
        return self._async.description

    @property
    def rowcount(self) -> int:
        return self._async.rowcount

    @property
    def lastrowid(self) -> int | None:
        return self._async.lastrowid

    @property
    def rownumber(self) -> int | None:
        return self._async.rownumber

    @property
    def arraysize(self) -> int:
        return self._async.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._async.arraysize = value

    @property
    def closed(self) -> bool:
        return self._async.closed

    @property
    def row_factory(self) -> RowFactory | None:
        return self._async.row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        self._async.row_factory = value

    @property
    def completed_iterations(self) -> int:
        return self._async.completed_iterations

    def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> Self:
        self._connection._run(self._async.execute(operation, parameters))
        return self

    def executemany(self, operation: str, seq_of_parameters: Iterable[Sequence[Any]], /) -> Self:
        self._connection._run(self._async.executemany(operation, seq_of_parameters))
        return self

    def fetchone(self) -> Any:
        self._connection._check_thread()
        self._async._check_open()
        return self._async._fetchone(self)

    def fetchmany(self, size: int | None = None) -> list[Any]:
        self._connection._check_thread()
        self._async._check_open()
        return self._async._fetchmany(self, size)

    def fetchall(self) -> list[Any]:
        self._connection._check_thread()
        self._async._check_open()
        return self._async._fetchall(self)

    def close(self) -> None:
        self._async.close()

    def setinputsizes(self, sizes: Sequence[Any] | None, /) -> None:
        self._async.setinputsizes(sizes)

    def setoutputsize(self, size: int | None, column: int | None = None, /) -> None:
        self._async.setoutputsize(size, column)

    def callproc(self, procname: str, parameters: Sequence[Any] | None = None, /) -> NoReturn:
        self._async.callproc(procname, parameters)

    def nextset(self) -> NoReturn:
        self._async.nextset()

    def scroll(self, value: int, mode: str = "relative", /) -> NoReturn:
        self._async.scroll(value, mode)

    def executescript(self, sql_script: str, /) -> NoReturn:
        self._async.executescript(sql_script)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Any:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        return f"<Cursor rowcount={self.rowcount} {state} at 0x{id(self):x}>"

    def __reduce__(self) -> NoReturn:
        raise TypeError(
            f"cannot pickle {type(self).__name__!r}: cursors reference a live connection; "
            "fetch the rows and pickle those instead"
        )
