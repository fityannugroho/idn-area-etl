"""Tests for CSV encoding bugs (BOM and header whitespace)."""

import csv
from pathlib import Path


def test_bom_handling_bug(tmp_path: Path) -> None:
    """Test that CSV files with UTF-8 BOM are read correctly.

    Bug: CSV files exported from Excel often have UTF-8 BOM (\ufeff).
    This prepends the BOM to the first header, causing "\\ufeffcode" instead of "code".
    This breaks all code lookups and forces incorrect fuzzy matching.
    """
    # Create CSV with UTF-8 BOM (common from Excel exports)
    csv_path = tmp_path / "test_with_bom.csv"
    csv_path.write_bytes(
        b"\xef\xbb\xbfcode,name\n"  # UTF-8 BOM + header
        b"11,Aceh\n"
        b"12,Sumatera Utara\n"
    )

    # Read with standard csv.DictReader using utf-8 encoding
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        # This is the BUG: first header has BOM prepended
        assert reader.fieldnames is not None
        first_header = reader.fieldnames[0]

        # BUG DEMONSTRATION: With utf-8 encoding, BOM is included in header
        assert first_header == "\ufeffcode", (
            f"Expected BOM bug to manifest, but got: {first_header!r}"
        )

        # This lookup fails because key is "code" but actual key is "\ufeffcode"
        assert "code" not in rows[0], "Lookup should fail with BOM in header"
        assert "\ufeffcode" in rows[0], "BOM is in the actual key"


def test_bom_handling_fix(tmp_path: Path) -> None:
    """Test that SafeDictReader fixes UTF-8 BOM handling.

    Solution: Use utf-8-sig encoding which automatically strips BOM,
    combined with SafeDictReader that strips whitespace.
    """
    from idn_area_etl.utils import SafeDictReader

    # Create CSV with UTF-8 BOM
    csv_path = tmp_path / "test_with_bom.csv"
    csv_path.write_bytes(
        b"\xef\xbb\xbfcode,name\n"  # UTF-8 BOM + header
        b"11,Aceh\n"
        b"12,Sumatera Utara\n"
    )

    # Read with SafeDictReader using utf-8-sig encoding
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = SafeDictReader(f)
        rows = list(reader)

        # FIX: utf-8-sig strips BOM automatically
        assert reader.fieldnames == ["code", "name"]

        # Lookups now work correctly
        assert "code" in rows[0]
        assert rows[0]["code"] == "11"
        assert rows[0]["name"] == "Aceh"


def test_header_whitespace_bug(tmp_path: Path) -> None:
    """Test that CSV headers with whitespace cause lookup failures.

    Bug: Headers with leading/trailing whitespace (e.g., "code ")
    cause dictionary key lookups to fail silently.
    """
    # Create CSV with whitespace in headers
    csv_path = tmp_path / "test_with_whitespace.csv"
    csv_path.write_text(
        "code ,name  \n"  # Headers with trailing whitespace
        "11,Aceh\n"
        "12,Sumatera Utara\n",
        encoding="utf-8",
    )

    # Read with standard csv.DictReader
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        # This is the BUG: headers have whitespace
        assert reader.fieldnames == ["code ", "name  "]

        # This lookup fails because key is "code" but actual key is "code "
        assert "code" not in rows[0], "Lookup should fail with whitespace"
        assert "code " in rows[0], "Actual key has trailing space"
        assert "name  " in rows[0], "Actual key has trailing spaces"


def test_header_whitespace_fix(tmp_path: Path) -> None:
    """Test that SafeDictReader fixes header whitespace handling.

    Solution: SafeDictReader strips leading/trailing whitespace from all headers.
    """
    from idn_area_etl.utils import SafeDictReader

    # Create CSV with whitespace in headers
    csv_path = tmp_path / "test_with_whitespace.csv"
    csv_path.write_text(
        "code ,name  \n"  # Headers with trailing whitespace
        "11,Aceh\n"
        "12,Sumatera Utara\n",
        encoding="utf-8",
    )

    # Read with SafeDictReader
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = SafeDictReader(f)
        rows = list(reader)

        # FIX: SafeDictReader strips whitespace from headers
        assert reader.fieldnames == ["code", "name"]

        # Lookups now work correctly
        assert "code" in rows[0]
        assert rows[0]["code"] == "11"
        assert rows[0]["name"] == "Aceh"


def test_combined_bom_and_whitespace_fix(tmp_path: Path) -> None:
    """Test that SafeDictReader handles both BOM and whitespace together."""
    from idn_area_etl.utils import SafeDictReader

    # Create CSV with BOTH BOM and whitespace in headers
    csv_path = tmp_path / "test_combined.csv"
    csv_path.write_bytes(
        b"\xef\xbb\xbfcode ,name  \n"  # UTF-8 BOM + headers with whitespace
        b"11,Aceh\n"
        b"12,Sumatera Utara\n"
    )

    # Read with SafeDictReader using utf-8-sig
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = SafeDictReader(f)
        rows = list(reader)

        # Both issues fixed
        assert reader.fieldnames == ["code", "name"]
        assert rows[0]["code"] == "11"
        assert rows[0]["name"] == "Aceh"
