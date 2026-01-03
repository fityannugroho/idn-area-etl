# IDN Area ETL - Agent Guidelines

This document provides essential information for AI agents working on the `idn-area-etl` repository.

## Overview
A Python CLI tool for extracting Indonesian area codes (Province, Regency, District, Village) and Island data from PDF tables into CSV files. Includes validation and normalization.

## Tech Stack
- **Language:** Python 3.12+
- **Task Runner:** `uv`
- **PDF Extraction:** `camelot-py`, `ghostscript`, `pypdf`
- **Data Processing:** `pandas`
- **CLI Framework:** `typer`
- **HTTP Client:** `httpx`
- **Fuzzy Matching:** `rapidfuzz`
- **Linting/Formatting:** `ruff`
- **Type Checking:** `pyright`
- **Testing:** `pytest`

## Documentation

### Rules
- [Coding Rules](.agent/rules/coding.md): Style, types, patterns, error handling
- [Testing Rules](.agent/rules/testing.md): Test commands, organization, best practices

### Workflows
- [Development Workflow](.agent/workflows/development.md): Setup, CLI commands, patterns, flows

## Quick Start

```bash
# Install dependencies
uv sync --all-extras

# Run checks
uv run ruff check .
uv run pyright

# Run tests
uv run pytest
```

## Configuration
See [`idnareaetl.toml`](idnareaetl.toml) for data configuration (batch_size, output_headers, filename_suffix).

## Key Modules
| File | Purpose |
|------|---------|
| `cli.py` | Typer CLI with extract, validate, normalize commands |
| `extractors.py` | PDF table extraction logic |
| `validator.py` | CSV validation layer |
| `normalizer.py` | Data normalization using ground truth |
| `ground_truth.py` | Hierarchical ground truth index |
| `remote.py` | Remote ground truth download/cache |
| `writer.py` | Buffered CSV writing |
| `utils.py` | Regex, normalization, fuzzy search utilities |
| `config.py` | TOML configuration loading |

## Instructions
- **Proactiveness:** When adding a new extractor, update `extractors.py` and register in `cli.py`.
- **Validation:** Always run `uv run ruff check .` and `uv run pyright` after modifications.
- **Testing:** Verify against `tests/fixtures/`. Add test cases for new patterns.
- **Safety:** Do not modify `uv.lock` manually; use `uv` commands.
- **CLI Changes:** Maintain signal handlers and progress bars (`tqdm`).
- **Documentation:** Use docstrings for public classes/methods.
- **Normalization:** Cover all statuses: `valid`, `corrected`, `ambiguous`, `not_found`.
- **Ground Truth:** Maintain hierarchical relationships correctly.
