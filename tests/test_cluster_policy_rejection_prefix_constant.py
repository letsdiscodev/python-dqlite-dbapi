"""CLUSTER_POLICY_REJECTION_PREFIX is the SSOT so a single
str(exc).startswith(CLUSTER_POLICY_REJECTION_PREFIX) check matches every raise site that
routes through cluster_policy_rejection_message.
"""

from __future__ import annotations

from pathlib import Path

import dqlitedbapi
from dqlitedbapi._constants import (
    CLUSTER_POLICY_REJECTION_PREFIX,
    cluster_policy_rejection_message,
)


def test_constant_is_module_exported() -> None:
    """CLUSTER_POLICY_REJECTION_PREFIX is re-exported on the public surface."""
    assert dqlitedbapi.CLUSTER_POLICY_REJECTION_PREFIX == "Cluster policy rejection"
    assert "CLUSTER_POLICY_REJECTION_PREFIX" in dqlitedbapi.__all__


def test_helper_short_form_starts_with_prefix() -> None:
    msg = cluster_policy_rejection_message(None, "policy says no")
    assert msg.startswith(CLUSTER_POLICY_REJECTION_PREFIX)
    assert msg == "Cluster policy rejection; policy says no"


def test_helper_long_form_starts_with_prefix() -> None:
    """The stage variant must also start with the bare prefix."""
    msg = cluster_policy_rejection_message("during leader discovery", "policy says no")
    assert msg.startswith(CLUSTER_POLICY_REJECTION_PREFIX)
    assert msg == "Cluster policy rejection during leader discovery; policy says no"


def test_no_string_literal_clones_in_source() -> None:
    """The literal "Cluster policy rejection" must not be copy-pasted back into a raise site."""
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
        # Allow the literal in docstrings/comments; only f-string raise-site clones are banned.
        assert 'f"Cluster policy rejection' not in text, (
            f"{path}: literal 'Cluster policy rejection' must route through "
            f"cluster_policy_rejection_message() so the prefix SSOT is honoured"
        )
