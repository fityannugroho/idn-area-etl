# Development Workflow

- Always use MCP Sequentialthinking. Research → Plan → Implement (don't jump straight to coding).
- Follow coding rules in [.agent/rules/coding.md](/.agent/rules/coding.md).
- Prefer simple, obvious solutions over clever abstractions.
- Always use `uv` as the package manager.
- After completing changes: run `uv run ruff check`, `uv run ruff format`, `uv run pyright`, and `uv run pytest`.
- If anything fails: stop, investigate, fix, and re-run checks.
- Do not proceed with further implementation until current implementation passes all checks
- Do not modify `uv.lock` manually; use `uv` commands.
