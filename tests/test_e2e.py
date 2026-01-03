import os
import subprocess
import sys
from pathlib import Path

import pytest


def _read_text_exact(path: Path) -> str:
    """
    Read file as raw bytes and decode as UTF-8 without any normalization.
    This preserves exact line endings and quoting so pytest shows a clean diff.
    """
    return path.read_bytes().decode("utf-8")


def _assert_csv_equal_as_text(expected: Path, actual: Path) -> None:
    exp = _read_text_exact(expected)
    act = _read_text_exact(actual)
    # Let pytest produce a unified diff on failure
    assert act == exp, f"CSV differs: {actual.name}"


@pytest.mark.e2e
def test_cli_e2e_extract_matches_expected(tmp_path: Path) -> None:
    fixtures = Path(__file__).parent / "fixtures"
    pdf_path = fixtures / "target_tables.pdf"
    assert pdf_path.exists(), "Missing fixture: tests/fixtures/target_tables.pdf"

    expected_files = {
        "province": fixtures / "expected_province.csv",
        "regency": fixtures / "expected_regency.csv",
        "district": fixtures / "expected_district.csv",
        "village": fixtures / "expected_village.csv",
        "island": fixtures / "expected_island.csv",
    }
    for key, p in expected_files.items():
        assert p.exists(), f"Missing golden file for {key}: {p}"

    # Build environment for subprocess; ensure 'src' is importable if not installed
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        (str(src_dir) + os.pathsep + env["PYTHONPATH"])
        if "PYTHONPATH" in env and env["PYTHONPATH"]
        else str(src_dir)
    )

    output_name = "e2e"
    cmd = [
        sys.executable,
        "-m",
        "idn_area_etl.cli",
        "extract",
        str(pdf_path),
        "--destination",
        str(tmp_path),
        "--output",
        output_name,
        "--chunk-size",
        "3",
    ]

    # Run the CLI as a real subprocess
    proc = subprocess.run(
        cmd,
        env=env,
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode == 0, (
        f"CLI failed (exit={proc.returncode})\n"
        f"--- STDOUT ---\n{proc.stdout}\n"
        f"--- STDERR ---\n{proc.stderr}\n"
    )

    actual_files = {
        "province": tmp_path / f"{output_name}.province.csv",
        "regency": tmp_path / f"{output_name}.regency.csv",
        "district": tmp_path / f"{output_name}.district.csv",
        "village": tmp_path / f"{output_name}.village.csv",
        "island": tmp_path / f"{output_name}.island.csv",
    }
    for key, p in actual_files.items():
        assert p.exists(), f"Missing CLI output for {key}: {p}"

    for key in actual_files.keys():
        _assert_csv_equal_as_text(expected_files[key], actual_files[key])
