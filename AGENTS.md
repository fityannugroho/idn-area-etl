# IDN Area ETL - Agent Guidelines

This document provides essential information for AI agents working on the `idn-area-etl` repository.

## Project Overview
A Python-based CLI tool for extracting Indonesian area codes (Province, Regency, District, Village) and Island data from PDF tables into structured CSV files.

## Tech Stack
- **Language:** Python 3.12+
- **Task Runner:** `uv`
- **PDF Extraction:** `camelot-py`, `ghostscript`, `pypdf`
- **Data Processing:** `pandas`
- **CLI Framework:** `typer`
- **Linting/Formatting:** `ruff`
- **Type Checking:** `pyright`
- **Testing:** `pytest`

## Development Workflow

### Environment Setup
```bash
# Install dependencies
uv sync --all-extras
```

### Build & Lint
```bash
# Run all checks (lint + type check)
uv run ruff check .
uv run pyright

# Build the package
uv build
```

### Testing
```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov

# Run a specific test file
uv run pytest tests/test_extractors.py

# Run a specific test function
uv run pytest tests/test_extractors.py::test_area_extractor_matches
```

## Coding Standards

Detailed rules are modularized in `.agent/rules/`:

- [`coding.md`](.agent/rules/coding.md): Style, types, patterns, error handling
- [`testing.md`](.agent/rules/testing.md): Test commands, organization, best practices

## Configuration (`idnareaetl.toml`)
The application uses a TOML file for configuration. Each area (province, regency, etc.) has its own data configuration:
- `batch_size`: Number of rows to buffer before writing to disk.
- `output_headers`: A sequence of strings for the CSV header.
- `filename_suffix`: Suffix appended to the output file name.

Example schema:
```toml
[data.province]
batch_size = 100
output_headers = ["code", "name"]
filename_suffix = "province"
```

## File Structure
- `src/idn_area_etl/`: Core source code.
    - `cli.py`: Typer CLI implementation, handles signal processing (SIGINT) and chunked PDF reading.
    - `extractors.py`: PDF table extraction logic (Base and specialized extractors).
    - `writer.py`: Buffered CSV writing logic with header support.
    - `utils.py`: Shared regex, normalization, and coordinate formatting utilities.
    - `config.py`: TOML configuration loading and schema definitions.
- `tests/`: Pytest suite.
    - `fixtures/`: Contains sample PDFs and expected CSV outputs for verification.
    - `test_extractors.py`: Unit tests for extraction logic.
    - `test_e2e.py`: End-to-end extraction tests using real-world fixtures.

## Common Patterns & Examples

### Adding a New Extractor
1. Create a new class inheriting from `TableExtractor` in `extractors.py`.
2. Define the `areas` frozenset with the area keys it handles.
3. Implement `matches(df)` to detect compatible tables.
4. Implement `_extract_rows(df)` to parse and return structured data.
5. Register the extractor in `cli.py` by instantiating it with a context manager.

### Extraction Flow
1. CLI reads PDF in chunks using `camelot.read_pdf()`.
2. For each table, iterate through registered extractors.
3. First matching extractor processes the table via `extract_and_write()`.
4. Extracted rows are buffered in `OutputWriter`.
5. When buffer reaches `batch_size`, data is flushed to CSV.
6. On completion or interrupt, all buffers are flushed and files closed.

## Instructions for Agents
- **Proactiveness:** When adding a new extractor, always update `extractors.py` and ensure it's registered or used in `cli.py`.
- **Validation:** Always run `uv run ruff check .` and `uv run pyright` after modifications.
- **Testing:** If modifying extraction logic, verify against `tests/fixtures/`. Add new test cases for new extraction patterns.
- **Safety:** Do not modify `uv.lock` manually; use `uv` commands.
- **CLI Changes:** When modifying the CLI, ensure signal handlers and progress bars (`tqdm`) are maintained.
- **Documentation:** Maintain clear docstrings for public classes and methods, focusing on the *why* for complex logic.
