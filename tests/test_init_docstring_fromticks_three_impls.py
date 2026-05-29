"""Doc-pin: dqlitedbapi.__doc__ distinguishes the three *FromTicks
constructors (date.fromtimestamp vs datetime.fromtimestamp, and each stdlib
slice) rather than lumping them, which would mislead on the date-only,
no-sub-second Date arm."""

from __future__ import annotations

import dqlitedbapi


def test_init_docstring_does_not_lump_under_datetime_fromtimestamp() -> None:
    """Regression guard: the pre-fix ``datetime.fromtimestamp(ticks)`` phrase
    that lumped all three constructors must not return."""
    doc = dqlitedbapi.__doc__ or ""
    assert "``datetime.fromtimestamp(ticks)``" not in doc, (
        "user-facing docstring still uses the pre-fix ``datetime."
        "fromtimestamp(ticks)`` phrase that lumped Date under the "
        "Timestamp delegate; readers infer all three return "
        "``datetime`` and that the Date arm preserves sub-second "
        "precision — neither is true."
    )
