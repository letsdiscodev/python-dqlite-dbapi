"""Pin: ``LEADER_ERROR_CODES`` is the single source of truth for the
leader-change SQLite extended codes, owned by ``dqlitewire``. The
dbapi layer does not import this constant today — it passes
leader-class failures through verbatim with the wire code intact and
relies on substring matching against the failure text instead. This
test pins the absence of a local copy: any future contributor who
mistakenly defines ``LEADER_ERROR_CODES`` inside dbapi (rather than
importing from the wire SSOT) will trip this check.

Mirrors the discipline in
``python-dqlite-client/tests/test_leader_error_codes_identity.py``
and ``sqlalchemy-dqlite/tests/test_leader_error_codes_identity.py``.
"""

from __future__ import annotations

import pkgutil

import dqlitedbapi


def test_dbapi_does_not_shadow_wire_leader_error_codes() -> None:
    # Walk every submodule of dqlitedbapi and confirm no module
    # defines ``LEADER_ERROR_CODES`` locally. A re-import from
    # ``dqlitewire`` would be acceptable as long as identity is
    # preserved — but the current dbapi shape avoids the import
    # altogether and the test asserts that posture.
    from dqlitewire import LEADER_ERROR_CODES as wire_codes

    for module_info in pkgutil.walk_packages(
        dqlitedbapi.__path__,
        prefix=f"{dqlitedbapi.__name__}.",
    ):
        module = __import__(module_info.name, fromlist=["_"])
        local = getattr(module, "LEADER_ERROR_CODES", None)
        if local is None:
            continue
        # If a future refactor DOES introduce a re-import here, it
        # must be by identity (the same wire-layer object), not a
        # copy.
        assert local is wire_codes, (
            f"{module_info.name} defines LEADER_ERROR_CODES as a local "
            f"copy (not the dqlitewire SSOT). Import the wire constant "
            f"directly: ``from dqlitewire import LEADER_ERROR_CODES``."
        )
