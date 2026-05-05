"""Shared module-private constants for ``dqlitedbapi`` and
``dqlitedbapi.aio``.

The PEP 249 module-level ``sqlite_version_info`` / ``sqlite_version``
attributes need to be advertised on both the sync and the async
import surfaces, with identical values. The previous shape duplicated
the literal across the two ``__init__.py`` modules, which works but
invites drift — a maintainer who bumps one side and forgets the other
gets a silently inconsistent surface where
``dqlitedbapi.sqlite_version`` and ``dqlitedbapi.aio.sqlite_version``
disagree.

Centralising the literal here is the single source of truth. Both
``__init__.py`` files re-export under the PEP 249 lowercase names.

The value MUST NOT advertise more than the SQLite version bundled in
dqlite upstream: the SQLAlchemy SQLite dialect gates feature code
paths on this tuple (RETURNING ≥ 3.35, STRICT ≥ 3.37, etc.), and
advertising a version the server does not actually ship produces SQL
the server rejects on the first query. The integration test
``tests/integration/test_sqlite_version_pin.py`` runs
``SELECT sqlite_version()`` against the live cluster and fails if
this constant is ahead of what the server reports.
"""

from typing import Final

SQLITE_VERSION_INFO: Final[tuple[int, int, int]] = (3, 35, 0)
SQLITE_VERSION: Final[str] = ".".join(str(v) for v in SQLITE_VERSION_INFO)
