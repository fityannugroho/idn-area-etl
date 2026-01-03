"""Unit tests for the validator module."""

from pathlib import Path

import pytest

from idn_area_etl.validator import (
    DistrictValidator,
    IslandValidator,
    ProvinceValidator,
    RegencyValidator,
    ValidationError,
    ValidationReport,
    VillageValidator,
    get_validator,
    validate_csv,
)

# =============================================================================
# Fixtures
# =============================================================================

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def dirty_province_csv() -> Path:
    return FIXTURES_DIR / "dirty_province.csv"


@pytest.fixture
def dirty_regency_csv() -> Path:
    return FIXTURES_DIR / "dirty_regency.csv"


@pytest.fixture
def dirty_island_csv() -> Path:
    return FIXTURES_DIR / "dirty_island.csv"


# =============================================================================
# ValidationError tests
# =============================================================================


class TestValidationError:
    def test_validation_error_creation(self):
        err = ValidationError(
            row_number=5,
            column="code",
            value="ABC",
            error_type="invalid_code_format",
            message="Code must be numeric",
        )
        assert err.row_number == 5
        assert err.column == "code"
        assert err.value == "ABC"
        assert err.error_type == "invalid_code_format"
        assert err.message == "Code must be numeric"


# =============================================================================
# ValidationReport tests
# =============================================================================


class TestValidationReport:
    def test_report_initialization(self):
        report = ValidationReport(area="province", total_rows=100)
        assert report.area == "province"
        assert report.total_rows == 100
        assert report.valid_rows == 0
        assert report.invalid_rows == 0
        assert report.errors == []

    def test_add_error(self):
        report = ValidationReport(area="province", total_rows=10)
        err = ValidationError(
            row_number=1, column="code", value="X", error_type="test", message="test"
        )
        report.add_error(err)
        assert len(report.errors) == 1
        assert report.has_errors() is True

    def test_has_errors_when_empty(self):
        report = ValidationReport(area="province", total_rows=10)
        assert report.has_errors() is False

    def test_summary(self):
        report = ValidationReport(area="province", total_rows=100)
        report.valid_rows = 95
        report.invalid_rows = 5
        summary = report.summary()
        assert "province" in summary
        assert "100" in summary
        assert "95" in summary
        assert "5" in summary

    def test_to_csv(self, tmp_path: Path):
        report = ValidationReport(area="province", total_rows=10)
        report.add_error(
            ValidationError(
                row_number=2,
                column="code",
                value="X",
                error_type="invalid",
                message="Test error",
            )
        )
        output_path = tmp_path / "report.csv"
        report.to_csv(output_path)

        assert output_path.exists()
        content = output_path.read_text()
        assert "row_number,column,value,error_type,message" in content
        assert "2,code,X,invalid,Test error" in content


# =============================================================================
# get_validator factory tests
# =============================================================================


class TestGetValidator:
    def test_get_province_validator(self):
        v = get_validator("province")
        assert isinstance(v, ProvinceValidator)

    def test_get_regency_validator(self):
        v = get_validator("regency")
        assert isinstance(v, RegencyValidator)

    def test_get_district_validator(self):
        v = get_validator("district")
        assert isinstance(v, DistrictValidator)

    def test_get_village_validator(self):
        v = get_validator("village")
        assert isinstance(v, VillageValidator)

    def test_get_island_validator(self):
        v = get_validator("island")
        assert isinstance(v, IslandValidator)


# =============================================================================
# ProvinceValidator tests
# =============================================================================


class TestProvinceValidator:
    def test_valid_row(self):
        v = ProvinceValidator()
        errors = v.validate_row({"code": "11", "name": "ACEH"}, 2)
        assert errors == []

    def test_invalid_code_length(self):
        v = ProvinceValidator()
        errors = v.validate_row({"code": "1", "name": "TEST"}, 2)
        assert len(errors) == 1
        assert errors[0].error_type == "invalid_code_length"

    def test_non_numeric_code(self):
        v = ProvinceValidator()
        errors = v.validate_row({"code": "AB", "name": "TEST"}, 2)
        assert len(errors) == 1
        assert errors[0].error_type == "invalid_code_format"

    def test_empty_name(self):
        v = ProvinceValidator()
        errors = v.validate_row({"code": "11", "name": ""}, 2)
        assert len(errors) == 1
        assert errors[0].error_type == "empty_value"

    def test_empty_code(self):
        v = ProvinceValidator()
        errors = v.validate_row({"code": "", "name": "ACEH"}, 2)
        assert len(errors) == 1
        assert errors[0].error_type == "empty_value"

    def test_validate_headers_valid(self):
        v = ProvinceValidator()
        err = v.validate_headers(["code", "name"])
        assert err is None

    def test_validate_headers_invalid(self):
        v = ProvinceValidator()
        err = v.validate_headers(["kode", "nama"])
        assert err is not None
        assert err.error_type == "invalid_headers"


# =============================================================================
# RegencyValidator tests
# =============================================================================


class TestRegencyValidator:
    def test_valid_row(self):
        v = RegencyValidator()
        errors = v.validate_row(
            {"code": "11.01", "province_code": "11", "name": "KAB. ACEH SELATAN"}, 2
        )
        assert errors == []

    def test_invalid_code_format(self):
        v = RegencyValidator()
        # Code without dot should be invalid
        errors = v.validate_row({"code": "1101", "province_code": "11", "name": "TEST"}, 2)
        assert any(e.error_type == "invalid_code_format" for e in errors)

    def test_invalid_code_format_wrong_pattern(self):
        v = RegencyValidator()
        # Wrong pattern (3 digits after dot)
        errors = v.validate_row({"code": "11.001", "province_code": "11", "name": "TEST"}, 2)
        assert any(e.error_type == "invalid_code_format" for e in errors)

    def test_parent_code_mismatch(self):
        v = RegencyValidator()
        # code starts with 11, but province_code is 12
        errors = v.validate_row({"code": "11.01", "province_code": "12", "name": "TEST"}, 2)
        assert any(e.error_type == "invalid_parent_code" for e in errors)

    def test_empty_province_code(self):
        v = RegencyValidator()
        errors = v.validate_row({"code": "11.01", "province_code": "", "name": "TEST"}, 2)
        assert any(e.error_type == "empty_value" for e in errors)


# =============================================================================
# DistrictValidator tests
# =============================================================================


class TestDistrictValidator:
    def test_valid_row(self):
        v = DistrictValidator()
        errors = v.validate_row(
            {"code": "11.01.01", "regency_code": "11.01", "name": "KECAMATAN"}, 2
        )
        assert errors == []

    def test_invalid_code_format(self):
        v = DistrictValidator()
        # Code without dots should be invalid
        errors = v.validate_row(
            {"code": "1101001", "regency_code": "11.01", "name": "KECAMATAN"}, 2
        )
        assert any(e.error_type == "invalid_code_format" for e in errors)

    def test_invalid_regency_code_format(self):
        v = DistrictValidator()
        # Regency code without dot should be invalid
        errors = v.validate_row(
            {"code": "11.01.01", "regency_code": "11010", "name": "KECAMATAN"}, 2
        )
        assert any(e.error_type == "invalid_code_format" for e in errors)

    def test_parent_code_mismatch(self):
        v = DistrictValidator()
        # code starts with 11.01 but regency_code is 11.02
        errors = v.validate_row(
            {"code": "11.01.01", "regency_code": "11.02", "name": "KECAMATAN"}, 2
        )
        assert any(e.error_type == "invalid_parent_code" for e in errors)


# =============================================================================
# VillageValidator tests
# =============================================================================


class TestVillageValidator:
    def test_valid_row(self):
        v = VillageValidator()
        errors = v.validate_row(
            {"code": "11.01.01.2001", "district_code": "11.01.01", "name": "DESA"}, 2
        )
        assert errors == []

    def test_invalid_code_format(self):
        v = VillageValidator()
        # Code without dots should be invalid
        errors = v.validate_row(
            {"code": "1101012001", "district_code": "11.01.01", "name": "DESA"}, 2
        )
        assert any(e.error_type == "invalid_code_format" for e in errors)

    def test_invalid_district_code_format(self):
        v = VillageValidator()
        # District code without dots should be invalid
        errors = v.validate_row(
            {"code": "11.01.01.2001", "district_code": "11010010", "name": "DESA"}, 2
        )
        assert any(e.error_type == "invalid_code_format" for e in errors)

    def test_parent_code_mismatch(self):
        v = VillageValidator()
        # code starts with 11.01.01 but district_code is 11.01.02
        errors = v.validate_row(
            {"code": "11.01.01.2001", "district_code": "11.01.02", "name": "DESA"}, 2
        )
        assert any(e.error_type == "invalid_parent_code" for e in errors)


# =============================================================================
# IslandValidator tests
# =============================================================================


class TestIslandValidator:
    def test_valid_row(self):
        v = IslandValidator()
        errors = v.validate_row(
            {
                "code": "11.01.40001",
                "regency_code": "11.01",
                "coordinate": "5°30'45.12\" N 95°20'30.45\" E",
                "is_populated": "1",
                "is_outermost_small": "0",
                "name": "Pulau Test",
            },
            2,
        )
        assert errors == []

    def test_valid_regencyless_island(self):
        """Islands with pattern NN.00.NNNNN can have empty regency_code."""
        v = IslandValidator()
        errors = v.validate_row(
            {
                "code": "11.00.40001",
                "regency_code": "",
                "coordinate": "5°30'45.12\" N 95°20'30.45\" E",
                "is_populated": "1",
                "is_outermost_small": "0",
                "name": "Pulau Test",
            },
            2,
        )
        assert errors == []

    def test_invalid_code_format(self):
        v = IslandValidator()
        errors = v.validate_row(
            {
                "code": "11.01.4000",  # Missing digit
                "regency_code": "11.01",
                "coordinate": "5°30'45.12\" N 95°20'30.45\" E",
                "is_populated": "1",
                "is_outermost_small": "0",
                "name": "Pulau Test",
            },
            2,
        )
        assert any(e.error_type == "invalid_code_format" for e in errors)

    def test_invalid_regency_code_format(self):
        v = IslandValidator()
        errors = v.validate_row(
            {
                "code": "11.01.40001",
                "regency_code": "11010",  # Without dot
                "coordinate": "5°30'45.12\" N 95°20'30.45\" E",
                "is_populated": "1",
                "is_outermost_small": "0",
                "name": "Pulau Test",
            },
            2,
        )
        assert any(e.error_type == "invalid_code_format" for e in errors)

    def test_invalid_coordinate(self):
        v = IslandValidator()
        errors = v.validate_row(
            {
                "code": "11.01.40001",
                "regency_code": "11.01",
                "coordinate": "invalid coordinate",
                "is_populated": "1",
                "is_outermost_small": "0",
                "name": "Pulau Test",
            },
            2,
        )
        assert any(e.error_type == "invalid_coordinate" for e in errors)

    def test_invalid_boolean_flags(self):
        v = IslandValidator()
        errors = v.validate_row(
            {
                "code": "11.01.40001",
                "regency_code": "11.01",
                "coordinate": "5°30'45.12\" N 95°20'30.45\" E",
                "is_populated": "2",
                "is_outermost_small": "3",
                "name": "Pulau Test",
            },
            2,
        )
        assert any(e.error_type == "invalid_boolean" for e in errors)
        assert len([e for e in errors if e.error_type == "invalid_boolean"]) == 2


# =============================================================================
# validate_csv integration tests
# =============================================================================


class TestValidateCsv:
    def test_validate_dirty_province(self, dirty_province_csv: Path):
        """Test validation of province CSV with known errors."""
        reports = list(validate_csv(dirty_province_csv, "province"))
        assert len(reports) > 0

        final_report = reports[-1]
        assert final_report.total_rows == 6
        assert final_report.has_errors()
        # Expected errors: short code, non-numeric code, empty name, empty code
        assert final_report.invalid_rows == 4

    def test_validate_dirty_regency(self, dirty_regency_csv: Path):
        """Test validation of regency CSV with known errors."""
        reports = list(validate_csv(dirty_regency_csv, "regency"))
        final_report = reports[-1]

        assert final_report.total_rows == 6
        assert final_report.has_errors()
        # Expected: short code, parent mismatch, empty name, empty parent
        assert final_report.invalid_rows == 4

    def test_validate_dirty_island(self, dirty_island_csv: Path):
        """Test validation of island CSV with known errors."""
        reports = list(validate_csv(dirty_island_csv, "island"))
        final_report = reports[-1]

        assert final_report.total_rows == 7
        assert final_report.has_errors()
        # Expected: invalid code, invalid coord, invalid populated, invalid outermost, empty name
        # Valid: row 1 (all valid), row 7 (regencyless island with empty regency_code)
        assert final_report.invalid_rows == 5

    def test_validate_with_invalid_headers(self, tmp_path: Path):
        """Test validation fails early with invalid headers."""
        csv_content = "kode,nama\n11,ACEH\n"
        csv_path = tmp_path / "invalid_headers.csv"
        csv_path.write_text(csv_content)

        reports = list(validate_csv(csv_path, "province"))
        final_report = reports[-1]

        assert final_report.has_errors()
        assert any(e.error_type == "invalid_headers" for e in final_report.errors)
        # Should stop after header validation
        assert final_report.total_rows == 0


class TestValidateCsvChunking:
    def test_chunked_progress(self, tmp_path: Path):
        """Test that validate_csv yields progress reports for large files."""
        # Create a CSV with 150 rows
        lines = ["code,name"]
        for i in range(150):
            lines.append(f"{i:02d},Province {i}")
        csv_path = tmp_path / "large.csv"
        csv_path.write_text("\n".join(lines))

        # Use small chunk size
        reports = list(validate_csv(csv_path, "province", chunk_size=50))

        # Should yield at least 3 progress reports (50, 100, 150 rows)
        assert len(reports) >= 3
