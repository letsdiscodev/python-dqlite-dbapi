"""Doc-pin: ``_datetime_from_iso8601``'s docstring lists the two-step
fallback (datetime -> time) the body runs, not a third date arm."""

from __future__ import annotations

from dqlitedbapi.types import _datetime_from_iso8601


def test_docstring_does_not_claim_three_fallback_steps() -> None:
    doc = _datetime_from_iso8601.__doc__ or ""
    assert "3. ``datetime.date.fromisoformat``" not in doc, (
        "_datetime_from_iso8601 docstring still lists a numbered third "
        "fallback step (``datetime.date.fromisoformat``) that the "
        "implementation never runs."
    )
    assert "parses the string with ``datetime.date.fromisoformat``" not in doc, (
        "_datetime_from_iso8601 docstring still asserts that the "
        "fallback parses with ``datetime.date.fromisoformat`` — the "
        "body has no such call (the bare-date case is handled by "
        "step 1's auto-widen on Python 3.11+)."
    )
