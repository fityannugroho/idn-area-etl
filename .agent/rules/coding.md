# Coding Rules

- Follow DRY (Don't Repeat Yourself) and KISS (Keep It Simple, Stupid) principles.
- Write modular, reusable functions and classes.
- Prefer clear function/variable names over inline comments.
- Naming conventions:
  - Classes: `PascalCase`
  - Functions/Variables: `snake_case`
  - Constants: `UPPER_SNAKE_CASE`
  - Private members: `_prefix_underscore`,
- Prefer Object-Oriented Programming over procedural where applicable.
- Prefer composition over inheritance.
- Follow SOLID principles.
- Avoid duck typing; prefer static typing to ensure type safety and clarity.
- Never change `ruff` and `pyright` configurations without approval.
- Don't use comments to disable linters unless it's the last resort; fix the underlying issue instead.

## Code Style & Quality
- Maximum 100 characters per line.
- Strictly follow code style enforced by `ruff`. Run `uv run ruff check` and `uv run ruff format`; fix reported issues.
- Use docstrings for all public API: functions, classes, methods, modules, constants. Focus on the *why* rather than the *what* for complex logic.
- Use Python 3.12+ type hinting syntax.
- Prefer `| None` over `Optional[T]`.
- Annotate all function signatures and public class members.
- Ensure `pyright` passes with no errors: `uv run pyright`

## Code Area Formats
| Area | Pattern | Example |
|------|---------|---------|
| Province | `NN` | `"11"` |
| Regency | `NN.NN` | `"11.01"` |
| District | `NN.NN.NN` | `"11.01.02"` |
| Village | `NN.NN.NN.NNNN` | `"11.01.02.2001"` |
| Island | `NN.NN.NNNNN` | `"11.01.40001"` |

## Error Handling & Resource Management
- Use standard Python exceptions.
- Custom exceptions in `config.py`: `ConfigError`, `UnsupportedFormatError`, `ParseError`.
- Use context manager for resource management (`__enter__`/`__exit__`).
- Implement fallback logic for ambiguous data (see `IslandExtractor._infer_columns`).
- Log warnings for recoverable errors; raise exceptions for critical failures.

## Signal Handling
- CLI handles `SIGINT` for graceful shutdown.
- Set `interrupted` flag and finish current chunk before exiting.

## Remote Data Fetching
- Use `httpx` for HTTP requests (not `urllib` or `requests`)
- Always show progress bars for downloads using `tqdm`
- Handle network errors gracefully with fallbacks to cached data
- Implement proper error messages for network failures
- Cache metadata in JSON format with ISO 8601 timestamps
- Respect GitHub API rate limits and support `GITHUB_TOKEN` env var
- Download timeout: 30 seconds default
- Cache validity: 7 days default
