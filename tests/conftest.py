import pytest
import pandas as pd
from pathlib import Path
import tempfile
from unittest.mock import Mock
from typing import List, Any, Callable


@pytest.fixture
def sample_pdf_data():
    """Sample PDF data for testing."""
    return {"total_pages": 10, "filename": "test_data.pdf"}


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame that matches target table structure."""
    data = [
        ["Kode", "Nama Provinsi"],
        ["", ""],
        ["11", "ACEH"],
        ["12", "SUMATERA UTARA"],
        ["1101", "SIMEULUE"],
        ["1102", "ACEH SINGKIL"],
        ["110101", "TEUPAH SELATAN"],
        ["110102", "SIMEULUE TIMUR"],
        ["1101011001", "LUGU"],
        ["1101011002", "LABUHAN BAJAU"],
    ]
    return pd.DataFrame(data)


@pytest.fixture
def invalid_dataframe():
    """Sample DataFrame that doesn't match target table structure."""
    data = [
        ["Column1", "Column2", "Column3"],
        ["Data1", "Data2", "Data3"],
        ["More", "Data", "Here"],
    ]
    return pd.DataFrame(data)


@pytest.fixture
def temp_directory():
    """Temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_pdf_file(temp_directory: Path):
    """Create a temporary PDF file for testing."""
    pdf_path = temp_directory / "test.pdf"
    # Create a dummy PDF file (just an empty file for testing)
    pdf_path.write_bytes(b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n")
    return pdf_path


@pytest.fixture
def mock_camelot_table():
    """Mock camelot table with sample data."""
    mock_table = Mock()
    mock_table.df = pd.DataFrame(
        [
            ["Kode", "Nama Provinsi"],
            ["", ""],
            ["11", "ACEH"],
            ["1101", "SIMEULUE"],
            ["110101", "TEUPAH SELATAN"],
            ["1101011001", "LUGU"],
        ]
    )
    return mock_table


@pytest.fixture
def mock_camelot_read_pdf(mock_camelot_table: Mock) -> Callable[..., List[Mock]]:
    """Mock camelot.read_pdf function."""

    def _mock_read_pdf(*args: Any, **kwargs: Any) -> List[Mock]:
        return [mock_camelot_table]

    return _mock_read_pdf


@pytest.fixture
def sample_area_data():
    """Sample area data for testing."""
    return [
        ("11", "ACEH"),
        ("12", "SUMATERA UTARA"),
        ("1101", "SIMEULUE"),
        ("1102", "ACEH SINGKIL"),
        ("110101", "TEUPAH SELATAN"),
        ("110102", "SIMEULUE TIMUR"),
        ("1101011001", "LUGU"),
        ("1101011002", "LABUHAN BAJAU"),
    ]
