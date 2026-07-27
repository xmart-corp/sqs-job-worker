# AGENTS.md

## Commands

Run all of the following before finishing. CI runs the same.

```console
$ uv sync --all-extras --all-groups
$ uv run pytest
$ uv run ruff check . && uv run ruff format --check .
$ uv run ty check src
```

## Conventions

- Keep the core framework-agnostic: no web framework, APM SDK, or deployment-platform API in the core.
- Inject vendor and platform behavior through framework-agnostic middleware and callbacks with null/empty defaults; adapters live in `contrib/`.
- Avoid unrelated or speculative refactors and one-use constants, helper classes, wrappers, or data structures unless reuse or semantics clearly requires them.
- Write comments and docstrings in English.
- Comment only what the code cannot say for itself; never restate what a variable name, a method name, or the adjacent code already makes obvious.
- In Markdown docs, prefer bullet lists over tables, and don't use a bold lead-in as a heading inside a bullet.

## Tests

- Check existing coverage first and prefer updating or consolidating tests.
- Add tests only for core behavior, public contracts, or credible regressions, in proportion to the change.
- Avoid dedicated tests for trivial details, unlikely defensive edges, or individual branches covered by one focused contract test.
