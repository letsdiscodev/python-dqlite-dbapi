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

**Pin contract — read before bumping**:

The constant is the *floor* the project guarantees, NOT the version
this dbapi was developed against. The SA dialect's
``_get_server_version_info`` (inherited from pysqlite) returns this
constant verbatim — there is no per-cluster handshake that consults
the actual SQLite shipped on a given dqlite server. SA gates feature
dispatch on this number alone:

  - ``insert_returning`` / ``update_returning`` / ``delete_returning``
    require ≥ 3.35.0.
  - STRICT-table compilation requires ≥ 3.37.0.
  - ``WITHOUT ROWID`` requires ≥ 3.8.2 (not relevant — far below floor).

If a future dqlite release bumps the bundled SQLite past 3.35.0, the
floor MAY be raised to unlock the corresponding SA feature dispatch
— but ONLY after the integration pin test confirms every supported
dqlite version ships at least the new floor. Bumping the floor in
this file alone is enough to silently break older-server clients.

If the cluster runs an *older* SQLite than this floor, SA dispatch
will produce queries the cluster rejects (e.g. RETURNING against a
sub-3.35 cluster). Operators must ensure their cluster's SQLite
version >= the value here.
"""

from typing import Final

SQLITE_VERSION_INFO: Final[tuple[int, int, int]] = (3, 35, 0)
SQLITE_VERSION: Final[str] = ".".join(str(v) for v in SQLITE_VERSION_INFO)
