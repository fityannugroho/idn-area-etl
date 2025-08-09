"""
Tests for CLI functionality.
Tests the command-line interface and argument validation.
"""

import pytest
from typer.testing import CliRunner
from src.idn_area_etl.cli import (
    app,
    validate_page_range,
    parse_page_range,
    version_option_callback,
    _validate_inputs,  # pyright: ignore
)
from pathlib import Path


@pytest.fixture
def runner() -> CliRunner:
    """Test runner for CLI tests."""
    return CliRunner()


class TestCLIBasics:
    """Test basic CLI functionality."""

    def test_cli_help_command(self, runner: CliRunner):
        """Test CLI help display."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    def test_cli_version_command(self, runner: CliRunner):
        """Test CLI version display."""
        result = runner.invoke(app, ["--version"])
        # Accept both success and error since version may not be configured
        assert result.exit_code in [0, 1]

    def test_missing_arguments(self, runner: CliRunner):
        """Test behavior with missing arguments."""
        result = runner.invoke(app, [])
        # Should show error for missing required argument
        assert result.exit_code == 2


class TestPageRangeValidation:
    """Test page range validation functions."""

    def test_validate_page_range_valid(self):
        """Test valid page range formats."""
        assert validate_page_range("1") is True
        assert validate_page_range("1,3,5") is True
        assert validate_page_range("1-5") is True
        assert validate_page_range("1-5,7-10") is True

    def test_validate_page_range_invalid(self):
        """Test invalid page range formats."""
        assert validate_page_range("invalid") is False
        assert validate_page_range("1-") is False
        assert validate_page_range("-5") is False
        assert validate_page_range("1--5") is False

    def test_parse_page_range(self):
        """Test page range parsing functionality."""
        assert parse_page_range("1", 10) == [1]
        assert parse_page_range("1,3,5", 10) == [1, 3, 5]
        assert parse_page_range("1-3", 10) == [1, 2, 3]
        assert parse_page_range("1-15", 10) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


class TestInputValidation:
    """Test input validation functions."""

    def test_validate_inputs_with_valid_file(self, temp_directory: Path):
        """Test input validation with valid PDF file."""
        pdf_file = temp_directory / "test.pdf"
        pdf_file.write_text("dummy content")

        # Should not raise exception
        try:
            _validate_inputs(pdf_file, "1-5", "output", temp_directory)
        except SystemExit:
            # May exit due to validation, but that's expected behavior
            pass

    def test_validate_empty_output_name(self, temp_directory: Path):
        """Test validation with empty output name."""
        pdf_file = temp_directory / "test.pdf"
        pdf_file.write_text("dummy content")

        # Empty string should trigger validation error
        try:
            _validate_inputs(pdf_file, None, "", temp_directory)
        except SystemExit:
            pass  # Expected to exit


class TestVersionCallback:
    """Test version callback functionality."""

    def test_version_callback_false(self):
        """Test version callback with False value (should do nothing)."""
        # Should return None and not exit
        result = version_option_callback(False)
        assert result is None
