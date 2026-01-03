# Coding Rules

## Style & Formatting

### Line Length
- Maximum 100 characters per line.

### Formatting
- Strictly follow `ruff` output.
- Run `uv run ruff check .` before committing.

### Imports
Order imports in three groups, separated by blank lines:
1. Standard library imports (e.g., `import re`, `from pathlib import Path`)
2. Third-party library imports (e.g., `import pandas as pd`, `import typer`)
3. Local application imports (e.g., `from idn_area_etl.config import Config`)

### Naming Conventions
| Type | Convention | Example |
|------|------------|---------|
| Classes | `PascalCase` | `TableExtractor`, `OutputWriter` |
| Functions/Variables | `snake_case` | `extract_and_write`, `page_range` |
| Constants | `UPPER_SNAKE_CASE` | `PROVINCE_CODE_LENGTH`, `RE_ISLAND_CODE` |
| Private members | `_prefix_underscore` | `_writers`, `_extract_rows` |

### Docstrings
- Use docstrings for all public classes and methods.
- Focus on the *why* rather than the *what* for complex logic.

---

## Type Hints

### Syntax
- Use Python 3.12+ type hinting syntax.
- Prefer `| None` over `Optional[T]`.
- Annotate all function signatures and public class members.
- Ensure `pyright` passes with no errors: `uv run pyright`

### Common Patterns
```python
# Union types - prefer this
def get_value(key: str) -> str | None:
    ...

# Immutable collections
areas: frozenset[Area] = frozenset({"province", "regency"})

# Type aliases
from typing import Literal
Area = Literal["province", "regency", "district", "village", "island"]

# Context manager signatures
from types import TracebackType

def __exit__(
    self,
    exc_type: type[BaseException] | None,
    exc: BaseException | None,
    tb: TracebackType | None,
) -> None:
    ...
```

---

## Data Processing Patterns

### Extractors
Extraction logic is encapsulated in `TableExtractor` subclasses in `extractors.py`.

| Method | Purpose |
|--------|---------|
| `matches(df)` | Determines if a table (DataFrame) should be processed by this extractor |
| `_extract_rows(df)` | Core logic to parse DataFrame into domain-specific rows |
| `extract_and_write(df)` | Orchestrates extraction and buffered writing |

Return type: `dict[Area, list[list[str]]]` mapping area keys to rows.

### DataFrames
- Use `pandas.DataFrame` for intermediate table representation.
- Tables are read using `camelot.read_pdf()` with the `lattice` flavor.
- Apply `.iloc` slicing to skip header rows and isolate data.

### Normalization
- **Coordinates:** Canonical format `'DD°MM'SS.ss" N DDD°MM'SS.ss" E'` via `format_coordinate()`.
- **Names:** Clean using `clean_name()` and `fix_wrapped_name()` from `utils.py`.
- **Headers:** Use `normalize_words()` for matching (handles "K o d e" → "Kode").

### Writing
- Use `OutputWriter` in `writer.py` for buffered CSV writing.
- Writers are managed via context managers in extractors.
- Call `flush()` when buffer reaches `batch_size` threshold.

### Code Areas
| Area | Pattern | Example |
|------|---------|---------|
| Province | `NN` | `"11"` |
| Regency | `NN.NN` | `"11.01"` |
| District | `NN.NN.NN` | `"11.01.02"` |
| Village | `NN.NN.NN.NNNN` | `"11.01.02.2001"` |
| Island | `NN.NN.NNNNN` | `"11.01.40001"` |

---

## Error Handling & Resource Management

### Exceptions
- Use standard Python exceptions.
- Custom exceptions in `config.py`: `ConfigError`, `UnsupportedFormatError`, `ParseError`.

### Context Managers
Use `__enter__`/`__exit__` for resource management:

```python
# TableExtractor pattern
class TableExtractor(ABC):
    def __enter__(self) -> "TableExtractor":
        self._open_outputs()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._close_outputs()

# Usage in CLI
with (
    AreaExtractor(destination, output_name, config) as area_extractor,
    IslandExtractor(destination, output_name, config) as island_extractor,
):
    extractors: list[TableExtractor] = [area_extractor, island_extractor]
    # Process tables...
```

### Graceful Fallbacks
- Implement fallback logic for ambiguous data (see `IslandExtractor._infer_columns`).
- Log warnings for recoverable errors; raise exceptions for critical failures.

### Signal Handling
- CLI handles `SIGINT` for graceful shutdown.
- Set `interrupted` flag and finish current chunk before exiting.

### Remote Data Fetching
- Use `httpx` for HTTP requests (not `urllib` or `requests`)
- Always show progress bars for downloads using `tqdm`
- Handle network errors gracefully with fallbacks to cached data
- Implement proper error messages for network failures
- Cache metadata in JSON format with ISO 8601 timestamps
- Respect GitHub API rate limits and support `GITHUB_TOKEN` env var
- Download timeout: 30 seconds default
- Cache validity: 7 days default
