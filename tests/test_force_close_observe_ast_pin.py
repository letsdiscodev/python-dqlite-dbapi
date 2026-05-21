"""AST pin for ``AsyncConnection.force_close_transport``'s nested
``_cancel_and_observe`` / ``_observe`` closures.

The existing ``test_force_close_cancel_and_observe_narrow_suppress``
matches on source substrings — a regression that moves the
``contextlib.suppress(Exception)`` literal to a different statement
position AND reintroduces a ``contextlib.suppress(BaseException)``
inside the ``_observe`` arm is invisible to the substring counts.
This file walks the ``force_close_transport``'s AST and asserts
every ``with contextlib.suppress(...)`` statement inside the
function (including nested closures) names ``Exception`` — NOT
``BaseException`` — so the narrow-suppress discipline is robust to
position refactoring.

The narrow-suppress discipline is project-wide and load-bearing:
``KeyboardInterrupt`` / ``SystemExit`` raised at the bytecode
boundary inside ``t.exception()`` must propagate to drive loop
shutdown. A widening to ``BaseException`` would swallow those
signals.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from dqlitedbapi.aio.connection import AsyncConnection


def _collect_suppress_args(tree: ast.AST) -> list[str]:
    """Return the name of the argument to every
    ``contextlib.suppress(...)`` call inside ``tree``. ``ast.walk``
    descends into nested function bodies — exactly what we want for
    closures defined inside the production function."""
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            call = item.context_expr
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            # Match both ``contextlib.suppress(X)`` and the bare
            # ``suppress(X)`` form (the production source uses the
            # qualified form throughout).
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name != "suppress":
                continue
            for arg in call.args:
                if isinstance(arg, ast.Name):
                    found.append(arg.id)
    return found


def test_force_close_transport_suppress_arms_all_narrow_to_exception() -> None:
    """Every ``contextlib.suppress(...)`` statement inside
    ``force_close_transport`` (including nested closures) names
    ``Exception``, NOT ``BaseException``. The narrow-suppress
    discipline must hold position-independently."""
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
