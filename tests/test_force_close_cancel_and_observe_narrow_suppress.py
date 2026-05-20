"""Pin: ``force_close_transport``'s ``_cancel_and_observe`` closure
suppresses ``Exception``, not ``BaseException``, so a signal-driven
``KeyboardInterrupt`` / ``SystemExit`` raised at the bytecode
boundary inside ``t.exception()`` still propagates and drives loop
shutdown.

Sibling to the client-side
``test_observe_drain_exception_narrow_suppress.py`` inspection pin
(commit ``d6d4792``). The existing
``test_force_close_observe_callback_coverage.py`` inlines the
``_observe`` body — that approach cannot catch a production-source
regression because the inlined copy is independent of the actual
``force_close_transport`` body. This pin inspects the production
source directly.
"""

from __future__ import annotations

import inspect


def test_force_close_transport_cancel_and_observe_uses_narrow_suppress() -> None:
    """The ``_cancel_and_observe`` closure inside
    ``AsyncConnection.force_close_transport`` must narrow-suppress
    ``Exception`` only. A regression widening to ``BaseException``
    would silently swallow a signal-driven KI / SystemExit at the
    callback's bytecode boundary."""
    from dqlitedbapi.aio.connection import AsyncConnection

    src = inspect.getsource(AsyncConnection.force_close_transport)
    assert "contextlib.suppress(BaseException)" not in src, (
        "force_close_transport's _cancel_and_observe must use narrow "
        "Exception suppression — see _observe_drain_exception sibling"
    )
    assert "contextlib.suppress(Exception)" in src
