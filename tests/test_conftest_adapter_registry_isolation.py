"""Pin: the autouse ``_restore_adapters`` fixture in conftest snapshots
and restores ``_ADAPTERS`` between tests.

A test that registers a custom adapter and then aborts mid-body must
not leak state into subsequent tests. Two cooperating tests verify
this: the first registers a probe adapter; the second observes that
the registry is clean again.
"""

from __future__ import annotations

from dqlitedbapi.types import _ADAPTERS, register_adapter


class _Probe:
    pass


def _adapter(_: _Probe) -> str:
    return "probe"


def test_step_one_register_then_leak_simulates_test_failure() -> None:
    """Step 1: register an adapter. The conftest fixture must restore."""
    register_adapter(_Probe, _adapter)
    assert _Probe in _ADAPTERS


def test_step_two_registry_is_clean_after_prior_test() -> None:
    """Step 2: the registry must not contain the prior test's probe."""
    assert _Probe not in _ADAPTERS
