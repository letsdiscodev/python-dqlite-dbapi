"""``iterdump`` stubs expose the stdlib-3.13 ``filter=`` kwarg via
``inspect.signature`` and carry no ``*args`` (stdlib is keyword-only after
``self``); ``**kwargs`` is deliberately kept as a forward-compat envelope.
"""

from __future__ import annotations

import inspect

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_iterdump_signature_exposes_filter_kwarg() -> None:
    sig = inspect.signature(dqlitedbapi.Connection.iterdump)
    assert "filter" in sig.parameters, (
        "iterdump stub should expose stdlib-3.13 ``filter=`` kwarg via "
        f"inspect.signature; got {sig!r}"
    )
    param = sig.parameters["filter"]
    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert param.default is None


def test_async_iterdump_signature_exposes_filter_kwarg() -> None:
    sig = inspect.signature(AsyncConnection.iterdump)
    assert "filter" in sig.parameters, (
        "AsyncConnection.iterdump stub should expose stdlib-3.13 "
        f"``filter=`` kwarg via inspect.signature; got {sig!r}"
    )
    param = sig.parameters["filter"]
    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert param.default is None


def test_sync_iterdump_signature_has_no_var_positional() -> None:
    """No ``*args``: stdlib's ``iterdump`` is ``(*, filter=None)``."""
    sig = inspect.signature(dqlitedbapi.Connection.iterdump)
    kinds = {p.kind for p in sig.parameters.values()}
    assert inspect.Parameter.VAR_POSITIONAL not in kinds, (
        f"iterdump must match stdlib ``(*, filter=None)`` (no positional "
        f"envelope after self); got {sig!r}"
    )


def test_async_iterdump_signature_has_no_var_positional() -> None:
    sig = inspect.signature(AsyncConnection.iterdump)
    kinds = {p.kind for p in sig.parameters.values()}
    assert inspect.Parameter.VAR_POSITIONAL not in kinds, (
        f"AsyncConnection.iterdump must match stdlib ``(*, filter=None)`` "
        f"(no positional envelope after self); got {sig!r}"
    )
