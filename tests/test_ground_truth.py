"""Unit tests for the ground_truth module."""

from pathlib import Path

import pytest

from idn_area_etl.ground_truth import (
    AreaIndex,
    AreaRecord,
    GroundTruthIndex,
    IslandIndex,
    IslandRecord,
)
from idn_area_etl.utils import MatchCandidate, fuzzy_search_top_n

# =============================================================================
# Fixtures
# =============================================================================

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_provinces() -> list[AreaRecord]:
    return [
        AreaRecord(code="11", name="ACEH"),
        AreaRecord(code="12", name="SUMATERA UTARA"),
        AreaRecord(code="13", name="SUMATERA BARAT"),
        AreaRecord(code="14", name="RIAU"),
        AreaRecord(code="15", name="JAMBI"),
    ]


@pytest.fixture
def sample_regencies() -> list[AreaRecord]:
    return [
        AreaRecord(code="11010", name="KAB. ACEH SELATAN", parent_code="11"),
        AreaRecord(code="11020", name="KAB. ACEH TENGGARA", parent_code="11"),
        AreaRecord(code="12010", name="KAB. DELI SERDANG", parent_code="12"),
        AreaRecord(code="12020", name="KAB. LANGKAT", parent_code="12"),
    ]


@pytest.fixture
def sample_islands() -> list[IslandRecord]:
    return [
        IslandRecord(
            code="11.01.40001",
            name="Pulau Weh",
            regency_code="11010",
            coordinate="5°30'45.12\" N 95°20'30.45\" E",
            is_populated="1",
            is_outermost_small="0",
        ),
        IslandRecord(
            code="11.01.40002",
            name="Pulau Breuh",
            regency_code="11010",
            coordinate="5°25'30.00\" N 95°15'20.00\" E",
            is_populated="1",
            is_outermost_small="1",
        ),
    ]


# =============================================================================
# fuzzy_search_top_n tests
# =============================================================================


class TestFuzzySearchTopN:
    def test_exact_match(self):
        choices = ["ACEH", "SUMATERA UTARA", "SUMATERA BARAT"]
        results = fuzzy_search_top_n("ACEH", choices)
        assert len(results) >= 1
        assert results[0].value == "ACEH"
        assert results[0].score == 100.0

    def test_fuzzy_match(self):
        choices = ["ACEH", "SUMATERA UTARA", "SUMATERA BARAT"]
        results = fuzzy_search_top_n("SUMATRA UTARA", choices)  # Typo
        assert len(results) >= 1
        assert results[0].value == "SUMATERA UTARA"
        assert results[0].score >= 80.0

    def test_with_keys(self):
        choices = ["ACEH", "SUMATERA UTARA"]
        keys = ["11", "12"]
        results = fuzzy_search_top_n("ACEH", choices, keys=keys)
        assert len(results) >= 1
        assert results[0].key == "11"

    def test_threshold_filtering(self):
        choices = ["ACEH", "SUMATERA UTARA"]
        results = fuzzy_search_top_n("XYZ", choices, threshold=90.0)
        assert len(results) == 0

    def test_limit_n(self):
        choices = ["A", "AA", "AAA", "AAAA", "AAAAA"]
        results = fuzzy_search_top_n("A", choices, n=2)
        assert len(results) == 2

    def test_empty_query(self):
        choices = ["ACEH", "SUMATERA UTARA"]
        results = fuzzy_search_top_n("", choices)
        assert results == []

    def test_empty_choices(self):
        results = fuzzy_search_top_n("ACEH", [])
        assert results == []


# =============================================================================
# AreaIndex tests
# =============================================================================


class TestAreaIndex:
    def test_add_and_get_by_code(self, sample_provinces: list[AreaRecord]):
        index = AreaIndex(area="province")
        for p in sample_provinces:
            index.add(p)

        result = index.get_by_code("11")
        assert result is not None
        assert result.name == "ACEH"

    def test_get_by_code_not_found(self, sample_provinces: list[AreaRecord]):
        index = AreaIndex(area="province")
        for p in sample_provinces:
            index.add(p)

        result = index.get_by_code("99")
        assert result is None

    def test_has_code(self, sample_provinces: list[AreaRecord]):
        index = AreaIndex(area="province")
        for p in sample_provinces:
            index.add(p)

        assert index.has_code("11") is True
        assert index.has_code("99") is False

    def test_search_by_name(self, sample_provinces: list[AreaRecord]):
        index = AreaIndex(area="province")
        for p in sample_provinces:
            index.add(p)

        results = index.search_by_name("ACEH")
        assert len(results) >= 1
        assert results[0].value == "ACEH"
        assert results[0].key == "11"

    def test_search_by_name_fuzzy(self, sample_provinces: list[AreaRecord]):
        index = AreaIndex(area="province")
        for p in sample_provinces:
            index.add(p)

        # Search with typo
        results = index.search_by_name("SUMATRA BARAT")
        assert len(results) >= 1
        assert results[0].value == "SUMATERA BARAT"

    def test_len(self, sample_provinces: list[AreaRecord]):
        index = AreaIndex(area="province")
        for p in sample_provinces:
            index.add(p)

        assert len(index) == 5


# =============================================================================
# IslandIndex tests
# =============================================================================


class TestIslandIndex:
    def test_add_and_get_by_code(self, sample_islands: list[IslandRecord]):
        index = IslandIndex()
        for island in sample_islands:
            index.add(island)

        result = index.get_by_code("11.01.40001")
        assert result is not None
        assert result.name == "Pulau Weh"

    def test_search_by_name(self, sample_islands: list[IslandRecord]):
        index = IslandIndex()
        for island in sample_islands:
            index.add(island)

        results = index.search_by_name("Pulau Weh")
        assert len(results) >= 1
        assert results[0].value == "Pulau Weh"


# =============================================================================
# GroundTruthIndex tests
# =============================================================================


class TestGroundTruthIndex:
    def test_initialization(self):
        gt = GroundTruthIndex()
        assert len(gt.provinces) == 0
        assert len(gt.regencies) == 0
        assert len(gt.districts) == 0
        assert len(gt.villages) == 0
        assert len(gt.islands) == 0

    def test_manual_population(
        self,
        sample_provinces: list[AreaRecord],
        sample_regencies: list[AreaRecord],
    ):
        gt = GroundTruthIndex()

        for p in sample_provinces:
            gt.add_area_record("province", p)

        for r in sample_regencies:
            gt.add_area_record("regency", r)

        assert len(gt.provinces) == 5
        assert len(gt.regencies) == 4

        # Test hierarchy query
        aceh_regencies = gt.get_regencies_for_province("11")
        assert len(aceh_regencies) == 2

    def test_search_name_in_context(
        self,
        sample_provinces: list[AreaRecord],
        sample_regencies: list[AreaRecord],
    ):
        gt = GroundTruthIndex()

        for p in sample_provinces:
            gt.add_area_record("province", p)

        for r in sample_regencies:
            gt.add_area_record("regency", r)

        # Search within province 11 (ACEH)
        results = gt.search_name_in_context("ACEH SELATAN", area="regency", parent_code="11")
        assert len(results) >= 1
        assert "ACEH SELATAN" in results[0].value

    def test_summary(self, sample_provinces: list[AreaRecord]):
        gt = GroundTruthIndex()
        for p in sample_provinces:
            gt.add_area_record("province", p)

        summary = gt.summary()
        assert "Provinces: 5" in summary
        assert "Regencies: 0" in summary


class TestGroundTruthIndexLoadFromDirectory:
    def test_load_expected_fixtures(self, tmp_path: Path):
        """Test loading from expected CSV fixtures."""
        # Create test CSVs
        province_csv = tmp_path / "test.province.csv"
        province_csv.write_text("code,name\n11,ACEH\n12,SUMATERA UTARA\n")

        regency_csv = tmp_path / "test.regency.csv"
        regency_csv.write_text(
            "code,province_code,name\n11010,11,KAB. ACEH SELATAN\n11020,11,KAB. ACEH TENGGARA\n"
        )

        island_csv = tmp_path / "islands.csv"
        island_csv.write_text(
            "code,regency_code,coordinate,is_populated,is_outermost_small,name\n"
            "11.01.40001,11010,5°30'45.12\" N 95°20'30.45\" E,1,0,Pulau Weh\n"
        )

        gt = GroundTruthIndex()
        gt.load_from_directory(tmp_path)

        assert len(gt.provinces) == 2
        assert len(gt.regencies) == 2
        assert len(gt.islands) == 1

        # Verify hierarchy was built
        aceh_regencies = gt.get_regencies_for_province("11")
        assert len(aceh_regencies) == 2

    def test_load_nonexistent_directory(self):
        gt = GroundTruthIndex()
        with pytest.raises(ValueError, match="Directory does not exist"):
            gt.load_from_directory(Path("/nonexistent/path"))

    def test_load_partial_data(self, tmp_path: Path):
        """Test loading when only some CSV files exist."""
        province_csv = tmp_path / "provinces.csv"
        province_csv.write_text("code,name\n11,ACEH\n")

        gt = GroundTruthIndex()
        gt.load_from_directory(tmp_path)

        assert len(gt.provinces) == 1
        assert len(gt.regencies) == 0  # No regency file


class TestMatchCandidate:
    def test_creation(self):
        mc = MatchCandidate(value="ACEH", score=95.5, key="11")
        assert mc.value == "ACEH"
        assert mc.score == 95.5
        assert mc.key == "11"

    def test_without_key(self):
        mc = MatchCandidate(value="ACEH", score=95.5)
        assert mc.key is None
