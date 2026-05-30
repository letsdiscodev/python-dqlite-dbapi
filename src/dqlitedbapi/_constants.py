"""Shared module-private constants for ``dqlitedbapi`` and ``dqlitedbapi.aio``.

``SQLITE_VERSION_INFO`` is the *floor* SQLite version the project guarantees, not the
version developed against: SA gates feature dispatch on it alone (RETURNING >= 3.35,
STRICT >= 3.37) with no per-cluster handshake. Only raise the floor after the integration
pin test confirms every supported dqlite server ships at least the new value, else older
clients silently break.
"""

from typing import Final, TypeGuard

__all__ = [
    "CLUSTER_POLICY_REJECTION_PREFIX",
    "SQLITE_VERSION",
    "SQLITE_VERSION_INFO",
    "cluster_policy_rejection_message",
]


def _is_int_not_bool(value: object) -> TypeGuard[int]:
    """True for a real int, rejecting bool (``isinstance(True, int)`` is True)."""
    return isinstance(value, int) and not isinstance(value, bool)


SQLITE_VERSION_INFO: Final[tuple[int, int, int]] = (3, 35, 0)
SQLITE_VERSION: Final[str] = ".".join(str(v) for v in SQLITE_VERSION_INFO)


# Shared so all producers match ``str(exc).startswith(CLUSTER_POLICY_REJECTION_PREFIX)``
# without importing client-layer types.
CLUSTER_POLICY_REJECTION_PREFIX: Final[str] = "Cluster policy rejection"


def cluster_policy_rejection_message(stage: str | None, inner: str) -> str:
    """Build the ``ClusterPolicyError`` rewrap message; ``stage`` is an optional suffix."""
    suffix = f" {stage}" if stage else ""
    return f"{CLUSTER_POLICY_REJECTION_PREFIX}{suffix}; {inner}"
