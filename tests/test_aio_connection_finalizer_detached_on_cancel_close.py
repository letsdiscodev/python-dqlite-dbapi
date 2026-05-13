"""Pin: ``AsyncConnection.close()``'s
``(asyncio.CancelledError, KeyboardInterrupt, SystemExit)`` arm
explicitly detaches ``self._finalizer`` — defensive parity with the
orderly close (top-of-close) and fork-short-circuit arms.

The orderly path already detaches the finalizer at the top of
``close()`` BEFORE entering the under-lock arm, so on the current
shape a runtime cancel-during-close lands with the finalizer
already detached. The pin is structural / defensive: a future
refactor that moves the orderly detach AFTER the await arm, or
that swaps the order of `self._closed = True` and the detach,
would re-introduce the asymmetry. Pinning the cancel arm to also
detach keeps the discipline explicit at every termination shape.

Companion to ``test_aio_connection_finalizer_detached_on_close``
which pins the orderly + fork-shortcut arms.
"""

from __future__ import annotations

import ast
import inspect

from dqlitedbapi.aio import connection as aio_conn_mod


def test_close_cancel_arm_source_pin_detaches_finalizer() -> None:
    """Source-level pin: the cancel/KI/SystemExit ``except`` arm
    body writes ``self._finalizer`` (the symmetric detach the
    orderly path also performs).

    Mirrors the AST-pin discipline used elsewhere (sibling
    ``test_async_transaction_owner_set_inside_try_frame``). The
    runtime-observed detach today comes from the orderly path's
    top-of-close detach; if a future refactor moves that detach
    AFTER the under-lock arm, the cancel-arm explicit detach is
    the only remaining defence against finalizer-registry
    accumulation under cancel-storm shutdowns.
    """
    src = inspect.getsource(aio_conn_mod.AsyncConnection.close)
    tree = ast.parse(src.lstrip())

    def _arm_writes_finalizer(handler: ast.ExceptHandler) -> bool:
        for node in ast.walk(handler):
            if isinstance(node, ast.Assign):
                for tgt in node.targets:
                    if isinstance(tgt, ast.Attribute) and tgt.attr == "_finalizer":
                        return True
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                func = node.value.func
                if isinstance(func, ast.Attribute) and func.attr == "detach":
                    # ``self._finalizer.detach()`` shape.
                    target = func.value
                    if isinstance(target, ast.Attribute) and target.attr == "_finalizer":
                        return True
        return False

    cancel_arm_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and node.type is not None:
            type_repr = ast.unparse(node.type)
            if "CancelledError" in type_repr and (
                "KeyboardInterrupt" in type_repr or "SystemExit" in type_repr
            ):
                cancel_arm_found = True
                assert _arm_writes_finalizer(node), (
                    f"AsyncConnection.close()'s cancel/KI/SystemExit arm "
                    f"must detach self._finalizer to match the orderly + "
                    f"fork-shortcut arms. Source unparse: {type_repr}"
                )
    assert cancel_arm_found, (
        "AsyncConnection.close() must carry an except "
        "(CancelledError, KeyboardInterrupt, SystemExit) arm; "
        "structural pin failed to find it"
    )
