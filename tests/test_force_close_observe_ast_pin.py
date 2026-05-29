"""AST pin: every ``contextlib.suppress(...)`` in ``force_close_transport`` (including
nested closures) names ``Exception``, not ``BaseException`` — position-independent, so
KI/SystemExit at the ``t.exception()`` boundary propagate to drive loop shutdown."""

from __future__ import annotations

import ast
import inspect
import textwrap

from dqlitedbapi.aio.connection import AsyncConnection


def _collect_suppress_args(tree: ast.AST) -> list[str]:
    """Names of the arg to every ``contextlib.suppress(...)`` in ``tree`` (nested too)."""
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            call = item.context_expr
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            # Match both qualified ``contextlib.suppress(X)`` and bare ``suppress(X)``.
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name != "suppress":
                continue
            for arg in call.args:
                if isinstance(arg, ast.Name):
                    found.append(arg.id)
    return found


def test_force_close_transport_suppress_arms_all_narrow_to_exception() -> None:
    src = textwrap.dedent(inspect.getsource(AsyncConnection.force_close_transport))
    tree = ast.parse(src)

    args = _collect_suppress_args(tree)
    assert args, (
        "Expected at least one ``contextlib.suppress(...)`` in "
        "``force_close_transport`` (the body and its nested "
        "``_cancel_and_observe`` / ``_observe`` closures own several). "
        "If zero, a refactor removed the suppress family entirely — "
        "verify the narrow-suppress invariant still holds at the "
        "new shape."
    )
    for arg_name in args:
        assert arg_name != "BaseException", (
            "force_close_transport has a contextlib.suppress(BaseException) "
            "statement; the narrow-suppress discipline forbids this "
            "(KeyboardInterrupt / SystemExit must propagate through "
            "done-callbacks). Either narrow to Exception or, if a wide "
            "suppress is truly required at a specific site, document the "
            "exception with an inline comment and exempt the line here."
        )
