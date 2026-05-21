"""Doc-pin: the user-facing ``dqlitedbapi.__doc__`` summary of the
three ``*FromTicks`` constructors mentions both ``datetime.date``
and ``datetime.datetime`` delegations and enumerates each stdlib
slice ``[:3]`` / ``[3:6]`` / ``[:6]`` rather than lumping them under
``[:6]``.

The pre-fix bullet read "DateFromTicks / TimeFromTicks /
TimestampFromTicks use ``datetime.fromtimestamp(ticks)`` rather
than stdlib's ``time.localtime(ticks)[:6]``. Sub-second precision
in fractional ``ticks`` is preserved in the returned ``datetime``".
The Date arm actually uses ``datetime.date.fromtimestamp`` and the
returned ``datetime.date`` cannot carry sub-second precision — the
blanket claim misleads porting readers consulting
``pydoc dqlitedbapi`` for cross-driver parity.
"""

from __future__ import annotations

import dqlitedbapi


def test_init_docstring_pins_three_distinct_fromticks_impls() -> None:
    doc = dqlitedbapi.__doc__ or ""
    # The Date arm uses ``datetime.date.fromtimestamp``; the Time
    # and Timestamp arms use ``datetime.datetime.fromtimestamp``.
    assert "datetime.date.fromtimestamp" in doc, (
        "user-facing docstring must mention the Date arm's "
        "``datetime.date.fromtimestamp`` delegate so porting readers "
        "see the return-type asymmetry vs Time/Timestamp."
    )
    assert "datetime.datetime.fromtimestamp" in doc, (
        "user-facing docstring must mention the Time/Timestamp "
        "delegate so porting readers see the sub-second-preserving "
        "form vs the Date arm's date-only return."
    )
    # The stdlib baseline must enumerate each slice — lumping all
    # three under ``[:6]`` misleads on the Date arm (stdlib does
    # ``[:3]``).
    for slice_token in ("[:3]", "[3:6]", "[:6]"):
        assert slice_token in doc, (
            f"user-facing docstring must mention stdlib slice "
            f"{slice_token!r} so the three constructors' baselines "
            f"are distinguishable on the porting side."
        )


def test_init_docstring_does_not_lump_under_datetime_fromtimestamp() -> None:
    """Regression guard: the pre-fix wording used a single
    ``datetime.fromtimestamp(ticks)`` phrase that implied all three
    constructors return ``datetime``. The post-fix wording splits
    the Date arm out, so the pre-fix exact phrase must not return.
    """
    doc = dqlitedbapi.__doc__ or ""
    # The exact pre-fix substring lumped all three; the post-fix
    # wording uses the more specific ``datetime.date.fromtimestamp``
    # and ``datetime.datetime.fromtimestamp`` (note the explicit
    # module-qualified names).
    assert "``datetime.fromtimestamp(ticks)``" not in doc, (
        "user-facing docstring still uses the pre-fix ``datetime."
        "fromtimestamp(ticks)`` phrase that lumped Date under the "
        "Timestamp delegate; readers infer all three return "
        "``datetime`` and that the Date arm preserves sub-second "
        "precision — neither is true."
    )
