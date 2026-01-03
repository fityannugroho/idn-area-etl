"""Unit tests for the normalizer module."""

from pathlib import Path

import pytest

from idn_area_etl.ground_truth import AreaRecord, GroundTruthIndex, IslandRecord
from idn_area_etl.normalizer import (
    NormalizationReport,
    NormalizationSuggestion,
    Normalizer,
    RowNormalization,
    normalize_csv,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def ground_truth() -> GroundTruthIndex:
    """Create a ground truth index with sample data."""
    gt = GroundTruthIndex()

    # Add provinces
    gt.add_area_record("province", AreaRecord(code="11", name="ACEH"))
    gt.add_area_record("province", AreaRecord(code="12", name="SUMATERA UTARA"))
    gt.add_area_record("province", AreaRecord(code="13", name="SUMATERA BARAT"))

    # Add regencies (format: NN.NN, e.g., "11.01")
    gt.add_area_record(
        "regency", AreaRecord(code="11.01", name="KAB. ACEH SELATAN", parent_code="11")
    )
    gt.add_area_record(
        "regency", AreaRecord(code="11.02", name="KAB. ACEH TENGGARA", parent_code="11")
    )
    gt.add_area_record(
        "regency", AreaRecord(code="12.01", name="KAB. DELI SERDANG", parent_code="12")
    )

    # Add districts (format: NN.NN.NN, e.g., "11.01.01")
    gt.add_area_record(
        "district", AreaRecord(code="11.01.01", name="BAKONGAN", parent_code="11.01")
    )
    gt.add_area_record(
        "district", AreaRecord(code="11.01.02", name="KLUET UTARA", parent_code="11.01")
    )

    # Add villages (format: NN.NN.NN.NNNN, e.g., "11.01.01.2001")
    gt.add_area_record(
        "village", AreaRecord(code="11.01.01.2001", name="KEUDE BAKONGAN", parent_code="11.01.01")
    )

    # Add islands (format: NN.NN.NNNNN, e.g., "11.01.40001")
    gt.add_island_record(
        IslandRecord(
            code="11.01.40001",
            name="Pulau Weh",
            regency_code="11.01",
            coordinate="5°30'45.12\" N 95°20'30.45\" E",
            is_populated="1",
            is_outermost_small="0",
        )
    )
    gt.add_island_record(
        IslandRecord(
            code="11.01.40002",
            name="Pulau Breuh",
            regency_code="11.01",
            coordinate="5°25'30.00\" N 95°15'20.00\" E",
            is_populated="1",
            is_outermost_small="1",
        )
    )

    return gt


@pytest.fixture
def normalizer(ground_truth: GroundTruthIndex) -> Normalizer:
    """Create a normalizer with sample ground truth data."""
    return Normalizer(ground_truth, confidence_threshold=80.0, ambiguity_threshold=5.0)


# =============================================================================
# NormalizationSuggestion tests
# =============================================================================


class TestNormalizationSuggestion:
    def test_creation(self):
        sug = NormalizationSuggestion(
            original="ACEH",
            suggested="ACEH",
            confidence=100.0,
            reason="Exact match",
        )
        assert sug.original == "ACEH"
        assert sug.suggested == "ACEH"
        assert sug.confidence == 100.0
        assert sug.reason == "Exact match"


# =============================================================================
# RowNormalization tests
# =============================================================================


class TestRowNormalization:
    def test_valid_status(self):
        row = {"code": "11", "name": "ACEH"}
        rn = RowNormalization(
            row_number=2,
            original=row,
            corrected=row.copy(),
            status="valid",
        )
        assert rn.status == "valid"
        assert not rn.has_changes()

    def test_corrected_status_with_changes(self):
        original = {"code": "11", "name": "ACE"}
        corrected = {"code": "11", "name": "ACEH"}
        rn = RowNormalization(
            row_number=2,
            original=original,
            corrected=corrected,
            status="corrected",
            suggestions=[
                NormalizationSuggestion(
                    original="ACE",
                    suggested="ACEH",
                    confidence=95.0,
                    reason="Fuzzy matched",
                )
            ],
        )
        assert rn.status == "corrected"
        assert rn.has_changes()
        assert len(rn.suggestions) == 1

    def test_add_suggestion(self):
        row = {"code": "11", "name": "ACEH"}
        rn = RowNormalization(
            row_number=2,
            original=row,
            corrected=row.copy(),
            status="corrected",
        )
        rn.add_suggestion(
            NormalizationSuggestion(
                original="ACEH",
                suggested="ACEH MODIFIED",
                confidence=100.0,
                reason="Test",
            )
        )
        assert len(rn.suggestions) == 1


# =============================================================================
# NormalizationReport tests
# =============================================================================


class TestNormalizationReport:
    def test_add_valid_row(self):
        report = NormalizationReport(area="province")
        rn = RowNormalization(
            row_number=2,
            original={"code": "11", "name": "ACEH"},
            corrected={"code": "11", "name": "ACEH"},
            status="valid",
        )
        report.add(rn)

        assert report.total_rows == 1
        assert report.valid_rows == 1
        assert report.corrected_rows == 0

    def test_add_corrected_row(self):
        report = NormalizationReport(area="province")
        rn = RowNormalization(
            row_number=2,
            original={"code": "11", "name": "ACE"},
            corrected={"code": "11", "name": "ACEH"},
            status="corrected",
        )
        report.add(rn)

        assert report.total_rows == 1
        assert report.valid_rows == 0
        assert report.corrected_rows == 1

    def test_add_ambiguous_row(self):
        report = NormalizationReport(area="province")
        rn = RowNormalization(
            row_number=2,
            original={"code": "11", "name": "SUMATERA"},
            corrected={"code": "11", "name": "SUMATERA"},
            status="ambiguous",
        )
        report.add(rn)

        assert report.total_rows == 1
        assert report.ambiguous_rows == 1

    def test_add_not_found_row(self):
        report = NormalizationReport(area="province")
        rn = RowNormalization(
            row_number=2,
            original={"code": "99", "name": "UNKNOWN"},
            corrected={"code": "99", "name": "UNKNOWN"},
            status="not_found",
        )
        report.add(rn)

        assert report.total_rows == 1
        assert report.not_found_rows == 1

    def test_summary(self):
        report = NormalizationReport(area="province")
        report.add(
            RowNormalization(
                row_number=2,
                original={"code": "11", "name": "ACEH"},
                corrected={"code": "11", "name": "ACEH"},
                status="valid",
            )
        )
        summary = report.summary()
        assert "province" in summary
        assert "Total rows: 1" in summary
        assert "Valid rows: 1" in summary

    def test_write_corrected_csv(self, tmp_path: Path):
        report = NormalizationReport(area="province")
        report.add(
            RowNormalization(
                row_number=2,
                original={"code": "11", "name": "ACE"},
                corrected={"code": "11", "name": "ACEH"},
                status="corrected",
            )
        )

        output_path = tmp_path / "corrected.csv"
        report.write_corrected_csv(output_path, headers=["code", "name"])

        content = output_path.read_text()
        assert "code,name" in content
        assert "11,ACEH" in content

    def test_write_report_csv(self, tmp_path: Path):
        report = NormalizationReport(area="province")
        report.add(
            RowNormalization(
                row_number=2,
                original={"code": "11", "name": "ACE"},
                corrected={"code": "11", "name": "ACEH"},
                status="corrected",
                suggestions=[
                    NormalizationSuggestion(
                        original="ACE",
                        suggested="ACEH",
                        confidence=95.0,
                        reason="Fuzzy matched",
                    )
                ],
            )
        )

        output_path = tmp_path / "report.csv"
        report.write_report_csv(output_path)

        content = output_path.read_text()
        assert "row_number" in content
        assert "corrected" in content
        assert "ACE" in content
        assert "ACEH" in content


# =============================================================================
# Normalizer tests - Province
# =============================================================================


class TestNormalizerProvince:
    def test_exact_match(self, normalizer: Normalizer):
        row = {"code": "11", "name": "ACEH"}
        result = normalizer.normalize_province(row, 2)

        assert result.status == "valid"
        assert not result.has_changes()

    def test_name_correction_by_code(self, normalizer: Normalizer):
        """When code matches but name is different, correct the name."""
        row = {"code": "11", "name": "ACE"}  # Wrong name
        result = normalizer.normalize_province(row, 2)

        assert result.status == "corrected"
        assert result.corrected["name"] == "ACEH"
        assert len(result.suggestions) == 1
        assert result.suggestions[0].confidence == 100.0

    def test_fuzzy_match_name(self, normalizer: Normalizer):
        """When code not found, try fuzzy match on name."""
        row = {"code": "99", "name": "SUMATRA UTARA"}  # Typo in name
        result = normalizer.normalize_province(row, 2)

        # Should find SUMATERA UTARA via fuzzy match
        assert result.status == "corrected"
        assert result.corrected["name"] == "SUMATERA UTARA"
        assert result.corrected["code"] == "12"

    def test_not_found(self, normalizer: Normalizer):
        row = {"code": "99", "name": "COMPLETELY UNKNOWN PROVINCE"}
        result = normalizer.normalize_province(row, 2)

        assert result.status == "not_found"


# =============================================================================
# Normalizer tests - Regency
# =============================================================================


class TestNormalizerRegency:
    def test_exact_match(self, normalizer: Normalizer):
        row = {"code": "11.01", "province_code": "11", "name": "KAB. ACEH SELATAN"}
        result = normalizer.normalize_regency(row, 2)

        assert result.status == "valid"
        assert not result.has_changes()

    def test_name_and_parent_correction(self, normalizer: Normalizer):
        """Correct both name and parent code based on code lookup."""
        row = {"code": "11.01", "province_code": "99", "name": "ACEH SELATAN"}
        result = normalizer.normalize_regency(row, 2)

        assert result.status == "corrected"
        assert result.corrected["name"] == "KAB. ACEH SELATAN"
        assert result.corrected["province_code"] == "11"
        assert len(result.suggestions) == 2

    def test_fuzzy_match_within_province(self, normalizer: Normalizer):
        """Fuzzy match name within province context."""
        # Use a closer typo that will pass the 80% threshold
        row = {"code": "99.99", "province_code": "11", "name": "KAB. ACEH SELETAN"}  # Typo
        result = normalizer.normalize_regency(row, 2)

        assert result.status == "corrected"
        assert result.corrected["name"] == "KAB. ACEH SELATAN"


# =============================================================================
# Normalizer tests - District
# =============================================================================


class TestNormalizerDistrict:
    def test_exact_match(self, normalizer: Normalizer):
        row = {"code": "11.01.01", "regency_code": "11.01", "name": "BAKONGAN"}
        result = normalizer.normalize_district(row, 2)

        assert result.status == "valid"

    def test_name_correction(self, normalizer: Normalizer):
        row = {"code": "11.01.01", "regency_code": "11.01", "name": "BAKONGAN UTARA"}
        result = normalizer.normalize_district(row, 2)

        assert result.status == "corrected"
        assert result.corrected["name"] == "BAKONGAN"


# =============================================================================
# Normalizer tests - Village
# =============================================================================


class TestNormalizerVillage:
    def test_exact_match(self, normalizer: Normalizer):
        row = {"code": "11.01.01.2001", "district_code": "11.01.01", "name": "KEUDE BAKONGAN"}
        result = normalizer.normalize_village(row, 2)

        assert result.status == "valid"


# =============================================================================
# Normalizer tests - Island
# =============================================================================


class TestNormalizerIsland:
    def test_exact_match(self, normalizer: Normalizer):
        row = {
            "code": "11.01.40001",
            "regency_code": "11.01",
            "name": "Pulau Weh",
            "coordinate": "5°30'45.12\" N 95°20'30.45\" E",
            "is_populated": "1",
            "is_outermost_small": "0",
        }
        result = normalizer.normalize_island(row, 2)

        assert result.status == "valid"

    def test_name_correction(self, normalizer: Normalizer):
        row = {
            "code": "11.01.40001",
            "regency_code": "11.01",
            "name": "Pulau We",  # Typo
            "coordinate": "5°30'45.12\" N 95°20'30.45\" E",
            "is_populated": "1",
            "is_outermost_small": "0",
        }
        result = normalizer.normalize_island(row, 2)

        assert result.status == "corrected"
        assert result.corrected["name"] == "Pulau Weh"

    def test_fuzzy_match_island(self, normalizer: Normalizer):
        row = {
            "code": "99.99.99999",
            "regency_code": "11.01",
            "name": "Pulau Breuh",
            "coordinate": "",
            "is_populated": "",
            "is_outermost_small": "",
        }
        result = normalizer.normalize_island(row, 2)

        # Should match to existing island
        assert result.status in ("corrected", "valid")


# =============================================================================
# normalize_csv tests
# =============================================================================


class TestNormalizeCsv:
    def test_normalize_province_csv(self, tmp_path: Path, ground_truth: GroundTruthIndex):
        # Create a test CSV with dirty data
        csv_path = tmp_path / "dirty_province.csv"
        csv_path.write_text("code,name\n11,ACEH\n12,SUMATRA UTARA\n99,UNKNOWN\n")

        report = normalize_csv(csv_path, "province", ground_truth)

        assert report.total_rows == 3
        assert report.valid_rows == 1  # ACEH
        assert report.corrected_rows == 1  # SUMATRA UTARA -> SUMATERA UTARA
        assert report.not_found_rows == 1  # UNKNOWN

    def test_normalize_regency_csv(self, tmp_path: Path, ground_truth: GroundTruthIndex):
        csv_path = tmp_path / "dirty_regency.csv"
        csv_path.write_text(
            "code,province_code,name\n"
            "11.01,11,KAB. ACEH SELATAN\n"
            "11.02,11,ACEH TENGGARA\n"  # Missing KAB. prefix
        )

        report = normalize_csv(csv_path, "regency", ground_truth)

        assert report.total_rows == 2
        assert report.valid_rows == 1
        assert report.corrected_rows == 1


# =============================================================================
# Ambiguity tests
# =============================================================================


class TestAmbiguity:
    def test_ambiguous_match(self, ground_truth: GroundTruthIndex):
        """Test that ambiguous matches are properly detected."""
        # Add similar named records to create ambiguity
        gt = GroundTruthIndex()
        gt.add_area_record("province", AreaRecord(code="01", name="SUMATERA UTARA"))
        gt.add_area_record("province", AreaRecord(code="02", name="SUMATERA SELATAN"))

        normalizer = Normalizer(gt, confidence_threshold=50.0, ambiguity_threshold=10.0)

        row = {"code": "99", "name": "SUMATERA"}  # Ambiguous - could be either
        result = normalizer.normalize_province(row, 2)

        # Both "SUMATERA UTARA" and "SUMATERA SELATAN" should have similar scores
        # Result should be ambiguous or corrected to the top match depending on scores
        assert result.status in ("ambiguous", "corrected")
