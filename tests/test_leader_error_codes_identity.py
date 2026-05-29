"""Pin: dbapi must not define a local ``LEADER_ERROR_CODES``; the wire SSOT owns it."""

from __future__ import annotations

import pkgutil

import dqlitedbapi


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
