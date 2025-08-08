# idn-area ETL - Test Suite

## Running Tests

### Install Dependencies
```bash
uv sync --dev
```

### Run All Tests
```bash
uv run pytest
```

### Run Tests with Coverage
```bash
uv run pytest --cov=src/idn_area_etl --cov-report=html --cov-report=term
```

### Run Specific Test Files
```bash
# Text processing tests
uv run pytest tests/test_text_processing.py

# DataFrame processing tests
uv run pytest tests/test_dataframe_processing.py

# CLI tests
uv run pytest tests/test_cli.py

# File operations tests
uv run pytest tests/test_file_operations.py

# Mocking and integration tests
uv run pytest tests/test_mocking_integration.py
```

### Run Tests with Different Options
```bash
# Verbose output
uv run pytest -v

# Stop on first failure
uv run pytest -x

# Show local variables in tracebacks
uv run pytest -l

# Run tests in parallel (requires pytest-xdist)
uv run pytest -n auto
```

## Test Structure

### Test Files
- `test_text_processing.py` - Tests for text cleaning and processing functions
- `test_dataframe_processing.py` - Tests for DataFrame operations and table processing
- `test_cli.py` - Tests for CLI commands and argument validation
- `test_file_operations.py` - Tests for CSV file operations and data integrity
- `test_mocking_integration.py` - Tests with mocked dependencies and integration scenarios
- `conftest.py` - Shared fixtures and test configuration

### Test Coverage Areas

#### Text Processing (`test_text_processing.py`)
- `clean_name()` function with various input scenarios
- `normalize_header()` function for header normalization
- `validate_page_range()` and `parse_page_range()` functions
- `format_duration()` function for time formatting
- Regular expression transformations

#### DataFrame Processing (`test_dataframe_processing.py`)
- `is_target_table()` function for table validation
- `extract_area_code_and_name_from_table()` function
- `chunked()` utility function
- Integration tests for complete DataFrame processing pipeline

#### CLI Functionality (`test_cli.py`)
- Command-line interface testing with typer
- Input validation functions
- Version callback functionality
- Integration tests for extract command

#### File Operations (`test_file_operations.py`)
- CSV file creation and structure validation
- Code length constants and hierarchy validation
- File encoding and permissions testing
- Data integrity and duplicate prevention

#### Mocking & Integration (`test_mocking_integration.py`)
- External dependency mocking (camelot, PyPDF, tqdm)
- Signal handling and interruption scenarios
- Error handling and edge cases
- Performance testing with large datasets

### Fixtures (`conftest.py`)
- `sample_pdf_data` - Mock PDF metadata
- `sample_dataframe` - Valid table structure for testing
- `invalid_dataframe` - Invalid table for negative tests
- `temp_directory` - Temporary directory for file operations
- `sample_pdf_file` - Temporary PDF file for testing
- `mock_camelot_table` - Mock camelot table object
- `sample_area_data` - Sample Indonesian area data

## Test Categories

### Unit Tests
- Individual function testing
- Input validation
- Text processing algorithms
- Data transformation logic

### Integration Tests
- CLI command execution
- File I/O operations
- External library integration
- End-to-end workflow testing

### Mock Tests
- External dependency isolation
- Error scenario simulation
- Performance testing
- Signal handling

## Coverage Goals
- Aim for >90% code coverage
- Test both positive and negative scenarios
- Include edge cases and boundary conditions
- Validate error handling paths

## Continuous Integration
Tests are designed to run in CI environments with:
- Isolated test environments
- Mocked external dependencies
- Temporary file handling
- Cross-platform compatibility
