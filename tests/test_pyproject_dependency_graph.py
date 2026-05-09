"""Pin: dbapi's `pyproject.toml` declares every cross-package
runtime dependency present in the source-tree import graph.

The dbapi package imports from `dqlitewire` directly (not just
through the client layer). The runtime dependency manifest must
match the import graph so a future release that decouples client
from wire (or any consumer that resolves dbapi's pins without
client transitively pinning wire) does not silently break at
first-error time inside `DqliteError.__init__`'s lazy import of
`dqlitewire._truncate`.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def _declared_runtime_deps() -> set[str]:
    data = tomllib.loads(PYPROJECT.read_text())
    deps = data["project"]["dependencies"]
    names: set[str] = set()
    for spec in deps:
        m = re.match(r"^([A-Za-z0-9_.-]+)", spec)
        if m:
            names.add(m.group(1).replace("_", "-").lower())
    return names


def test_pyproject_declares_dqlite_wire_runtime_dep() -> None:
    """dqlitewire is imported throughout dbapi src/; the dependency
    declaration must mention it explicitly rather than rely on
    dqlite-client transitively pulling it in.
    """
    declared = _declared_runtime_deps()
    assert "dqlite-wire" in declared, (
        "dbapi imports `dqlitewire` directly (exceptions.py, types.py, "
        "cursor.py, connection.py, aio/* — verified via grep) but "
        "`pyproject.toml` `dependencies` does not list `dqlite-wire`. "
        "Today's resolution succeeds only because dqlite-client "
        "transitively pins it; that guarantee evaporates the moment a "
        "future client release decouples or a resolver sees a sibling "
        "without that transitive edge."
    )


def test_pyproject_declares_dqlite_client_runtime_dep() -> None:
    declared = _declared_runtime_deps()
    assert "dqlite-client" in declared
