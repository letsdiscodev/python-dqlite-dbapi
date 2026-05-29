# Development Guide

## Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) - Fast Python package manager
- Docker (for integration tests)

## Setup

Clone [dqlite-wire](https://github.com/letsdiscodev/python-dqlite-wire) and [dqlite-client](https://github.com/letsdiscodev/python-dqlite-client) alongside this checkout; `[tool.uv.sources]` in `pyproject.toml` points the sibling packages at `../python-dqlite-wire` and `../python-dqlite-client`, so `uv sync` picks up in-tree changes automatically.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv sync --extra dev
```

## Development Tools

| Tool | Purpose | Command |
|------|---------|---------|
| **pytest** | Testing framework | `pytest` |
| **ruff** | Linter (replaces flake8, isort, etc.) | `ruff check` |
| **ruff format** | Code formatter (replaces black) | `ruff format` |
| **mypy** | Static type checker | `mypy src` |

## Running Tests

```bash
# Run unit tests only
.venv/bin/pytest tests/ --ignore=tests/integration

# Run all tests (requires Docker cluster)
cd ../dqlite-test-cluster && docker compose up -d
.venv/bin/pytest tests/
```

## Linting & Formatting

```bash
# Lint
.venv/bin/ruff check src tests

# Auto-fix lint issues
.venv/bin/ruff check --fix src tests

# Format
.venv/bin/ruff format src tests
```

## Type Checking

```bash
.venv/bin/mypy src
```

## Comments and docstrings

Keep comments and docstrings to a minimum — the code should be clear
enough to stand on its own. Prefer renaming a function or variable over
writing a comment to explain an unclear one.

- Write a comment only when it captures something genuinely non-obvious
  that the code cannot: a subtle invariant, a security caveat, a
  workaround for an upstream bug, or a "why" a reader would otherwise
  get wrong. Delete comments that merely restate what the code does.
- Most functions need no docstring. Add a one-line docstring only when
  it tells a reader something the name and signature do not. Avoid
  multi-paragraph essays, param-by-param prose, and "Notes/Divergence"
  sections.
- Do not record rationale, history, or decision logs in comments — that
  context belongs in the commit message and git history (`git blame`,
  `git log -p`), where it stays attached to the change instead of aging
  in the source.
- Tooling directives (`# type:`, `# noqa`, `# pragma:`) are exempt —
  keep them.

When in doubt, leave it out: a missing explanation is a `git blame`
away; a redundant or stale comment is noise every future reader pays
for.

## Pre-commit Workflow

```bash
.venv/bin/ruff format src tests
.venv/bin/ruff check --fix src tests
.venv/bin/mypy src
.venv/bin/pytest tests/ --ignore=tests/integration
```

## PEP 249 Compliance

This package implements the DB-API 2.0 specification (PEP 249):

- `apilevel = "2.0"`
- `threadsafety = 1` (threads may share module, not connections)
- `paramstyle = "qmark"` (question mark placeholders)
