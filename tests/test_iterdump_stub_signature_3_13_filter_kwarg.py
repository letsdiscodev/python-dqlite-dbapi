"""Pin: ``iterdump`` stub exposes the stdlib-3.13 ``filter=`` kwarg
via ``inspect.signature`` so cross-driver tooling that walks the
dbapi-connection API surface (documentation generators, IDE
auto-complete, compatibility-shim detection) sees the documented
stdlib shape rather than the bare ``(*args, **kwargs)`` envelope.

The runtime parity — ``conn.iterdump(filter="x")`` not leaking
``TypeError`` — is already covered by
``test_stub_signatures_normalize_to_dbapi_error``. This file is the
introspection-parity follow-up.

Additionally pins that the iterdump signature carries NO
``VAR_POSITIONAL`` (``*args``) parameter — stdlib's iterdump is
keyword-only after ``self``. ``VAR_KEYWORD`` (``**kwargs``) is
deliberately KEPT per architect triage as a forward-compat envelope
for future Python additions; this test must therefore allow it.
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
    """Stdlib's ``iterdump`` is ``(*, filter=None)`` — keyword-only
    after ``self``. A ``*args`` envelope deviates from that shape and
    leaks a ``VAR_POSITIONAL`` parameter to introspection that stdlib
    does not. ``**kwargs`` is deliberately KEPT for forward-compat."""
    sig = inspect.signature(dqlitedbapi.Connection.iterdump)
    kinds = {p.kind for p in sig.parameters.values()}
    assert inspect.Parameter.VAR_POSITIONAL not in kinds, (
        f"iterdump must match stdlib ``(*, filter=None)`` (no positional "
        f"envelope after self); got {sig!r}"
    )


def test_async_iterdump_signature_has_no_var_positional() -> None:
    """Async-sibling pin for the no-``*args`` invariant."""
    sig = inspect.signature(AsyncConnection.iterdump)
    kinds = {p.kind for p in sig.parameters.values()}
    assert inspect.Parameter.VAR_POSITIONAL not in kinds, (
        f"AsyncConnection.iterdump must match stdlib ``(*, filter=None)`` "
        f"(no positional envelope after self); got {sig!r}"
    )
