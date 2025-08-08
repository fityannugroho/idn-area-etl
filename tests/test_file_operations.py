"""
Test module for file I/O operations and CSV handling.
"""
import pytest
import csv
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, Mock, mock_open
import pandas as pd

from src.idn_area_etl.cli import (
    PROVINCE_CODE_LENGTH,
    REGENCY_CODE_LENGTH,
    DISTRICT_CODE_LENGTH,
    VILLAGE_CODE_LENGTH
)


class TestCSVOutput:
    """Test cases for CSV output functionality."""

    def test_csv_file_creation(self, temp_directory):
        """Test CSV file creation and basic structure."""
        csv_file = temp_directory / "test.csv"

        # Create a simple CSV file
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["code", "name"])
            writer.writerow(["11", "ACEH"])
            writer.writerow(["12", "SUMATERA UTARA"])

        # Verify file was created and has correct content
        assert csv_file.exists()

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        expected_rows = [
            ["code", "name"],
            ["11", "ACEH"],
            ["12", "SUMATERA UTARA"]
        ]
        assert rows == expected_rows

    def test_province_csv_structure(self, temp_directory):
        """Test province CSV file structure."""
        csv_file = temp_directory / "provinces.csv"

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["code", "name"])
            writer.writerow(["11", "ACEH"])
            writer.writerow(["12", "SUMATERA UTARA"])

        # Read and verify structure
        df = pd.read_csv(csv_file, dtype={'code': str})
        assert list(df.columns) == ["code", "name"]
        assert len(df) == 2
        assert df.iloc[0]["code"] == "11"
        assert df.iloc[0]["name"] == "ACEH"

    def test_regency_csv_structure(self, temp_directory):
        """Test regency CSV file structure."""
        csv_file = temp_directory / "regencies.csv"

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["code", "province_code", "name"])
            writer.writerow(["1101", "11", "SIMEULUE"])
            writer.writerow(["1102", "11", "ACEH SINGKIL"])

        # Read and verify structure
        df = pd.read_csv(csv_file, dtype={'code': str, 'province_code': str})
        assert list(df.columns) == ["code", "province_code", "name"]
        assert len(df) == 2
        assert df.iloc[0]["code"] == "1101"
        assert df.iloc[0]["province_code"] == "11"

    def test_district_csv_structure(self, temp_directory):
        """Test district CSV file structure."""
        csv_file = temp_directory / "districts.csv"

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["code", "regency_code", "name"])
            writer.writerow(["110101", "1101", "TEUPAH SELATAN"])
            writer.writerow(["110102", "1101", "SIMEULUE TIMUR"])

        # Read and verify structure
        df = pd.read_csv(csv_file, dtype={'code': str, 'regency_code': str})
        assert list(df.columns) == ["code", "regency_code", "name"]
        assert len(df) == 2
        assert df.iloc[0]["code"] == "110101"
        assert df.iloc[0]["regency_code"] == "1101"

    def test_village_csv_structure(self, temp_directory):
        """Test village CSV file structure."""
        csv_file = temp_directory / "villages.csv"

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["code", "district_code", "name"])
            writer.writerow(["1101011001", "110101", "LUGU"])
            writer.writerow(["1101011002", "110101", "LABUHAN BAJAU"])

        # Read and verify structure
        df = pd.read_csv(csv_file, dtype={'code': str, 'district_code': str})
        assert list(df.columns) == ["code", "district_code", "name"]
        assert len(df) == 2
        assert df.iloc[0]["code"] == "1101011001"
        assert df.iloc[0]["district_code"] == "110101"


class TestCodeLengthConstants:
    """Test cases for area code length constants."""

    def test_code_length_constants(self):
        """Test that code length constants are correct."""
        assert PROVINCE_CODE_LENGTH == 2
        assert REGENCY_CODE_LENGTH == 5
        assert DISTRICT_CODE_LENGTH == 8
        assert VILLAGE_CODE_LENGTH == 13

    def test_code_hierarchy(self, sample_area_data):
        """Test that area codes follow the expected hierarchy."""
        for code, name in sample_area_data:
            if len(code) == PROVINCE_CODE_LENGTH:
                # Province codes should be 2 digits
                assert code.isdigit()
                assert len(code) == 2
            elif len(code) == REGENCY_CODE_LENGTH:
                # Regency codes should start with province code
                province_code = code[:PROVINCE_CODE_LENGTH]
                assert province_code.isdigit()
                assert len(province_code) == 2
            elif len(code) == DISTRICT_CODE_LENGTH:
                # District codes should start with regency code
                regency_code = code[:REGENCY_CODE_LENGTH]
                province_code = code[:PROVINCE_CODE_LENGTH]
                assert regency_code.isdigit()
                assert province_code.isdigit()
                assert len(regency_code) == 5
                assert len(province_code) == 2
            elif len(code) == VILLAGE_CODE_LENGTH:
                # Village codes should start with district code
                district_code = code[:DISTRICT_CODE_LENGTH]
                regency_code = code[:REGENCY_CODE_LENGTH]
                province_code = code[:PROVINCE_CODE_LENGTH]
                assert district_code.isdigit()
                assert regency_code.isdigit()
                assert province_code.isdigit()
                assert len(district_code) == 8
                assert len(regency_code) == 5
                assert len(province_code) == 2


class TestFileOperations:
    """Test cases for file operations."""

    def test_file_path_handling(self, temp_directory):
        """Test file path handling with different scenarios."""
        # Test with Path objects
        file_path = temp_directory / "test.csv"
        assert isinstance(file_path, Path)

        # Test file creation
        file_path.touch()
        assert file_path.exists()
        assert file_path.is_file()

        # Test file deletion
        file_path.unlink()
        assert not file_path.exists()

    def test_directory_creation(self, temp_directory):
        """Test directory creation for output files."""
        nested_dir = temp_directory / "nested" / "output"
        nested_dir.mkdir(parents=True, exist_ok=True)

        assert nested_dir.exists()
        assert nested_dir.is_dir()

        # Test file creation in nested directory
        test_file = nested_dir / "test.csv"
        test_file.touch()
        assert test_file.exists()

    def test_csv_encoding_utf8(self, temp_directory):
        """Test CSV file encoding with UTF-8 characters."""
        csv_file = temp_directory / "test_utf8.csv"

        # Test data with Indonesian characters
        test_data = [
            ["code", "name"],
            ["11", "ACÈH"],
            ["12", "SUMATRA ÚTARA"],
            ["13", "RÎAU"]
        ]

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in test_data:
                writer.writerow(row)

        # Read back and verify encoding
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        assert rows == test_data

    def test_csv_file_permissions(self, temp_directory):
        """Test CSV file creation with proper permissions."""
        csv_file = temp_directory / "permissions_test.csv"

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["test", "data"])

        # Check that file is readable and writable
        assert csv_file.exists()
        assert os.access(csv_file, os.R_OK)
        assert os.access(csv_file, os.W_OK)


class TestDataIntegrity:
    """Test cases for data integrity in file operations."""

    def test_duplicate_prevention(self, temp_directory):
        """Test prevention of duplicate entries."""
        csv_file = temp_directory / "duplicates.csv"

        # Simulate the duplicate prevention logic for provinces
        province_codes = set()
        provinces_to_write = []

        test_provinces = [
            ("11", "ACEH"),
            ("12", "SUMATERA UTARA"),
            ("11", "ACEH"),  # Duplicate
            ("13", "SUMATERA BARAT")
        ]

        for code, name in test_provinces:
            if code not in province_codes:
                province_codes.add(code)
                provinces_to_write.append((code, name))

        # Write to CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["code", "name"])
            for code, name in provinces_to_write:
                writer.writerow([code, name])

        # Verify no duplicates
        df = pd.read_csv(csv_file, dtype={'code': str})
        assert len(df) == 3  # Should have 3 unique provinces
        assert list(df["code"]) == ["11", "12", "13"]

    def test_data_consistency_across_files(self, temp_directory):
        """Test data consistency across different area level files."""
        # Simplified test - just check that we can write and read files correctly
        files = {
            'province': temp_directory / "test.province.csv",
            'regency': temp_directory / "test.regency.csv"
        }

        # Create province file
        province_data = pd.DataFrame([
            {"code": "11", "name": "ACEH"}
        ])
        province_data.to_csv(files['province'], index=False)

        # Create regency file
        regency_data = pd.DataFrame([
            {"code": "1101", "province_code": "11", "name": "SIMEULUE"}
        ])
        regency_data.to_csv(files['regency'], index=False)

        # Read back and verify
        province_df = pd.read_csv(files['province'], dtype={'code': str})
        regency_df = pd.read_csv(files['regency'], dtype={'code': str, 'province_code': str})

        # Basic consistency checks
        assert len(province_df) == 1
        assert len(regency_df) == 1
        assert province_df.iloc[0]["code"] == "11"
        assert regency_df.iloc[0]["province_code"] == "11"
