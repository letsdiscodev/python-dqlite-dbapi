"""Pin: ``CLUSTER_POLICY_REJECTION_PREFIX`` is the SSOT for the
documented "callers can branch on the message prefix without
importing client-layer types" recipe.

Three previously-divergent raise sites (sync connect-time leader-
discovery, sync connect-time post-construct, cursor-path rewrap)
now route through ``cluster_policy_rejection_message`` so a single
``str(exc).startswith(CLUSTER_POLICY_REJECTION_PREFIX)`` check
matches every producer. Mirror of the ``FAILED_TO_CONNECT_PREFIX``
and ``WIRE_DECODE_FAILED_PREFIX`` precedents.
"""

from __future__ import annotations

from pathlib import Path

import dqlitedbapi
from dqlitedbapi._constants import (
    CLUSTER_POLICY_REJECTION_PREFIX,
    cluster_policy_rejection_message,
)


def test_constant_is_module_exported() -> None:
    """``dqlitedbapi.CLUSTER_POLICY_REJECTION_PREFIX`` is part of the
    public surface (re-exported from ``__init__.py``)."""
    assert dqlitedbapi.CLUSTER_POLICY_REJECTION_PREFIX == "Cluster policy rejection"
    assert "CLUSTER_POLICY_REJECTION_PREFIX" in dqlitedbapi.__all__


def test_helper_short_form_starts_with_prefix() -> None:
    msg = cluster_policy_rejection_message(None, "policy says no")
    assert msg.startswith(CLUSTER_POLICY_REJECTION_PREFIX)
    assert msg == "Cluster policy rejection; policy says no"


def test_helper_long_form_starts_with_prefix() -> None:
    """The ``stage`` variant must also start with the bare prefix so
    the documented branching recipe matches."""
    msg = cluster_policy_rejection_message("during leader discovery", "policy says no")
    assert msg.startswith(CLUSTER_POLICY_REJECTION_PREFIX)
    assert msg == "Cluster policy rejection during leader discovery; policy says no"


def test_no_string_literal_clones_in_source() -> None:
    """A repo-walk: the literal "Cluster policy rejection" must NOT
    appear in production source outside the SSOT constant + helper
    definitions. Catches a future maintainer who copy-pastes the
    literal back into a raise site."""
    src_root = Path(__file__).parent.parent / "src" / "dqlitedbapi"
    paths = [
        src_root / "connection.py",
        src_root / "cursor.py",
        src_root / "aio" / "connection.py",
        src_root / "aio" / "cursor.py",
    ]
    for path in paths:
        if not path.exists():
            continue
        text = path.read_text()
        # Allow the literal in docstrings/comments referring to the
        # documented prefix contract, but disallow ``f"Cluster policy
        # rejection`` raise-site literals.
        assert 'f"Cluster policy rejection' not in text, (
            f"{path}: literal 'Cluster policy rejection' must route through "
            f"cluster_policy_rejection_message() so the prefix SSOT is honoured"
        )
