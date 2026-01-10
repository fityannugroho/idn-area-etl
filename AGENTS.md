# IDN Area ETL - Agent Guidelines

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

## Commands
- `uv sync --all-extras`: Install dependencies
- `uv run ruff check`: Run linting checks
- `uv run ruff format`: Run code formatting
- `uv run pyright`: Run type checking
- `uv run pytest`: Run all tests
- `uv run pytest --cov`: Run tests with coverage
- `uv build`: Build the package

## Rules and Workflows
- Follow all rules specified in `.agent/rules` directory.
- Follow the workflow specified in `.agent/workflows` directory.

## Project Structure
- `src/idn_area_etl/` - Main source code
  - `cli.py` - Typer CLI with extract, validate, normalize commands
  - `config.py` - TOML configuration loading
  - `utils.py` - Regex, normalization, fuzzy search utilities
  - `idnareaetl.toml` - Default runtime configuration
- `tests/` - Test suite
  - `fixtures/` - Sample PDFs and expected CSV outputs
- `pyproject.toml` - Project dependencies and metadata
- `pyrightconfig.json` - Type checking configuration
