"""
Pytest configuration and setup for test execution.
"""

# Makefile equivalent commands as Python script
test_commands = """
# Install development dependencies
uv sync --dev

# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src/idn_area_etl --cov-report=html --cov-report=term

# Run specific test file
uv run pytest tests/test_text_processing.py

# Run tests with verbose output
uv run pytest -v

# Run tests and stop on first failure
uv run pytest -x

# Run tests with specific markers (if any)
# uv run pytest -m "not slow"

# Run tests in parallel (if pytest-xdist is installed)
# uv run pytest -n auto
"""

if __name__ == "__main__":
    print("Available test commands:")
    print(test_commands)
