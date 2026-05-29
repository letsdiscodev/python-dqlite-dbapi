"""``_ensure_loop``'s lockless fast path snapshots ``self._loop`` into a local before
``.is_closed()`` so a racing ``close()`` nulling it cannot raise AttributeError (DCL defect).
"""

from __future__ import annotations

import threading

import dqlitedbapi


def test_ensure_loop_fast_path_tolerates_concurrent_loop_null() -> None:
    """Race ``_ensure_loop`` against a thread nulling ``self._loop`` mid-read; the
    snapshot pattern must never raise AttributeError on ``.is_closed()``."""
    conn = dqlitedbapi.connect.__wrapped__ if hasattr(dqlitedbapi.connect, "__wrapped__") else None
    if conn is None:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)
    else:
        from dqlitedbapi.connection import Connection

        conn = Connection.__new__(Connection)

    import asyncio
    import os

    conn._loop = asyncio.new_event_loop()
    conn._thread = None
    conn._loop_lock = threading.Lock()
    conn._closed = False
    conn._creator_pid = os.getpid()
    conn._creator_thread = threading.get_ident()
    conn._inner_finalize_handle = []
    conn._closed_flag = [False]
    conn._address = ""
    conn._close_timeout = 1.0
    conn._finalizer = None

    errors: list[BaseException] = []
    runs = [True]

    def racer() -> None:
        while runs[0]:
            old = conn._loop
            conn._loop = None
            if old is not None:
                conn._loop = old  # restore

    def caller() -> None:
        for _ in range(2000):
            try:
                result = (
                    conn._ensure_loop.__wrapped__(conn)
                    if hasattr(conn._ensure_loop, "__wrapped__")
                    else conn._ensure_loop()
                )
                assert result is not None or conn._loop is None
            except BaseException as e:  # noqa: BLE001
                errors.append(e)
                return

    t_caller = threading.Thread(target=caller)
    t_racer = threading.Thread(target=racer, daemon=True)
    t_caller.start()
    t_racer.start()
    t_caller.join(timeout=5.0)
    runs[0] = False
    t_racer.join(timeout=1.0)

    attr_errors = [e for e in errors if isinstance(e, AttributeError) and "is_closed" in str(e)]
    assert not attr_errors, (
        f"_ensure_loop fast path raised AttributeError on .is_closed() "
        f"reading nulled self._loop — DCL defect not fixed. Got "
        f"{len(attr_errors)} errors: {attr_errors[:3]}"
    )

    if conn._loop is not None and not conn._loop.is_closed():
        conn._loop.close()
