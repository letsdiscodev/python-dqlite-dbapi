"""Pin: ``dqlitedbapi`` top-level logger has a ``logging.NullHandler``.
The ``aio`` sub-package inherits via propagation; no separate handler.
See sibling test in ``python-dqlite-wire``."""

from __future__ import annotations

import logging

import dqlitedbapi  # noqa: F401 -- import for side effect
import dqlitedbapi.aio  # noqa: F401 -- async surface must propagate too


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
