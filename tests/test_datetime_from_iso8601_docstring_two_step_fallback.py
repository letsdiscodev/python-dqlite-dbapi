"""Doc-pin: ``_datetime_from_iso8601``'s docstring lists the two
fallback steps that the implementation actually runs, not three.

The body at ``types.py`` only tries ``datetime.datetime.fromisoformat``
followed by ``datetime.time.fromisoformat`` and raises ``DataError``.
There is no third ``date.fromisoformat`` arm — the bare-date case is
served by step 1 on Python 3.11+ (``datetime.fromisoformat("YYYY-MM-DD")``
returns midnight). The previous docstring enumerated a third step and
asserted "the fallback path parses the string with
``datetime.date.fromisoformat``" — neither claim was true. This pin
locks the docstring to the actual two-step shape so future readers
trust what's written.

Documentation-only; no behaviour change.
"""

from __future__ import annotations

from dqlitedbapi.types import _datetime_from_iso8601


def test_docstring_does_not_claim_three_fallback_steps() -> None:
    doc = _datetime_from_iso8601.__doc__ or ""
    # The old wording numbered "3. ``datetime.date.fromisoformat``"
    # as a fallback step that the body never runs. The new wording
    # must not contain a numbered "3." fallback step.
    assert "3. ``datetime.date.fromisoformat``" not in doc, (
        "_datetime_from_iso8601 docstring still lists a numbered third "
        "fallback step (``datetime.date.fromisoformat``) that the "
        "implementation never runs."
    )
    # The old "fallback path parses the string with
    # ``datetime.date.fromisoformat``" claim must also be gone.
    assert "parses the string with ``datetime.date.fromisoformat``" not in doc, (
        "_datetime_from_iso8601 docstring still asserts that the "
        "fallback parses with ``datetime.date.fromisoformat`` — the "
        "body has no such call (the bare-date case is handled by "
        "step 1's auto-widen on Python 3.11+)."
    )


def test_docstring_enumerates_two_step_fallback() -> None:
    doc = _datetime_from_iso8601.__doc__ or ""
    assert "Two-step fallback" in doc or "two-step fallback" in doc, (
        "_datetime_from_iso8601 docstring must explicitly state that "
        "the fallback is two-step (datetime → time)."
    )
