"""Pin eager ``import dqlitewire`` on dbapi load: a lazy wire import would
silently drop the wire-layer free-threading guard at this entry point."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path


def _run_subprocess_and_assert_wire_loaded(import_target: str) -> None:
    repo_src = Path(__file__).resolve().parent.parent / "src"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(repo_src)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    )
    snippet = f"""
        import sys
        assert "dqlitewire" not in sys.modules
        import {import_target}  # noqa: F401
        if "dqlitewire" not in sys.modules:
            print("FAIL: {import_target} did not transitively load dqlitewire", flush=True)
            sys.exit(1)
        print("OK", flush=True)
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(snippet)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"transitive-wire-import pin failed for {import_target}:\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    assert "OK" in result.stdout


def test_dqlitewire_loaded_after_dqlitedbapi_import() -> None:
    _run_subprocess_and_assert_wire_loaded("dqlitedbapi")


def test_dqlitewire_loaded_after_dqlitedbapi_aio_import() -> None:
    _run_subprocess_and_assert_wire_loaded("dqlitedbapi.aio")
