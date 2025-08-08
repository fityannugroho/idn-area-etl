"""
Test module for text processing functions in the CLI module.
"""
import pytest
from src.idn_area_etl.cli import (
    clean_name,
    normalize_words,
    validate_page_range,
    parse_page_range,
    format_duration,
    _apply_regex_transformations,
    fix_wrapped_name,
)


class TestCleanName:
    """Test cases for the clean_name function."""

    def test_clean_name_basic(self):
        """Test basic name cleaning."""
        assert clean_name("ACEH") == "ACEH"
        assert clean_name("SUMATERA UTARA") == "SUMATERA UTARA"

    def test_clean_name_with_digits_and_newlines(self):
        """Test cleaning names with digits and newlines."""
        assert clean_name("123\nACEH") == "ACEH"
        assert clean_name("ACEH\n456") == "ACEH"
        assert clean_name("123\nSUMATERA\nUTARA\n456") == "SUMATERA UTARA"

    def test_clean_name_with_leading_digits_space(self):
        """Test cleaning names with leading digits and spaces."""
        assert clean_name("1 ACEH") == "ACEH"
        assert clean_name("123 SUMATERA UTARA") == "SUMATERA UTARA"

    def test_clean_name_with_multiple_spaces(self):
        """Test cleaning names with multiple spaces."""
        assert clean_name("SUMATERA  UTARA") == "SUMATERA UTARA"
        assert clean_name("ACEH   BESAR") == "ACEH BESAR"

    def test_clean_name_empty_or_invalid(self):
        """Test cleaning empty or invalid names."""
        assert clean_name("") == ""
        assert clean_name("   ") == ""
        assert clean_name(None) == ""
        assert clean_name(123) == ""

    def test_clean_name_complex_case(self):
        """Test cleaning complex cases with multiple issues."""
        dirty_name = "123\nSUMATE\nRA  \n\n  UTARA\n456"
        expected = "SUMATE RA UTARA"
        assert clean_name(dirty_name) == expected

class TestWrappedText:
    """Test cases for the fix_wrapped_name function."""

    def test_fix_wrapped_text_basic(self):
        """Test fixing basic wrapped text."""
        assert fix_wrapped_name("ACEH") == "ACEH"
        assert fix_wrapped_name("SUMATERA UTARA") == "SUMATERA UTARA"

    def test_fix_wrapped_name_with_newlines(self):
        """Test fixing wrapped text with newlines."""
        assert fix_wrapped_name("ACEH\nBESAR") == "ACEH\nBESAR"
        assert fix_wrapped_name("SUMATERA\nUTARA") == "SUMATERA\nUTARA"

    def test_fix_wrapped_name_with_multiple_newlines(self):
        """Test fixing wrapped text with multiple newlines."""
        assert fix_wrapped_name("ACEH\n\nBESAR") == "ACEH\nBESAR"
        assert fix_wrapped_name("LINE1\n\nLINE2\n\nLINE3") == "LINE1\nLINE2\nLINE3"

    def test_fix_wrapped_name_empty_or_invalid(self):
        """Test fixing empty or invalid wrapped text."""
        assert fix_wrapped_name("") == ""
        assert fix_wrapped_name("   ") == ""
        assert fix_wrapped_name(None) == ""
        assert fix_wrapped_name(123) == ""

    def test_fix_wrapped_name_actual_data(self):
        """Test fixing wrapped text with actual data."""
        assert fix_wrapped_name("Sibarani Nasampulu/Namungk\nup") == "Sibarani Nasampulu/Namungkup"
        assert fix_wrapped_name("Kedungpomahanwet\nan") == "Kedungpomahanwetan"
        assert fix_wrapped_name("Kedungpomahankulo\nn") == "Kedungpomahankulon"
        assert fix_wrapped_name("Leragawi/Megagiraku\nk") == "Leragawi/Megagirakuk"
        assert fix_wrapped_name("Bakungtemenggunga\nn") == "Bakungtemenggungan"

    def test_fix_wrapped_name_with_correct_data(self):
        """Test fixing wrapped text with correct data."""
        assert fix_wrapped_name("Ompu Raja Hutapea\nTimur") == "Ompu Raja Hutapea\nTimur"
        assert fix_wrapped_name("Ompu Raja Hutapea\nTimur") != "Ompu Raja HutapeaTimur"
        assert fix_wrapped_name("Teungoh Glumpang\nVII") == "Teungoh Glumpang\nVII"
        assert fix_wrapped_name("Perkebunan Sungai\nIyu") == "Perkebunan Sungai\nIyu"
        assert fix_wrapped_name("Limba U I") == "Limba U I"

class TestNormalizeWords:
    """Test cases for the normalize_words function."""

    def test_normalize_words_valid_phrases(self):
        """Test normalizing valid words phrases."""
        assert normalize_words("Nama Provinsi") == "Nama Provinsi"
        assert normalize_words("Kode Wilayah") == "Kode Wilayah"
        assert normalize_words("Data Valid") == "Data Valid"

    def test_normalize_words_invalid_phrases(self):
        """Test normalizing invalid words phrases (misparsed)."""
        assert normalize_words("K o d e") == "Kode"
        assert normalize_words("K E T E R A N G A N") == "KETERANGAN"
        assert normalize_words("A B C D E F") == "ABCDEF"
        assert normalize_words("N A M A / J U M L A H") == "NAMA/JUMLAH"

    def test_normalize_words_edge_cases(self):
        """Test normalizing edge cases."""
        assert normalize_words("") == ""
        assert normalize_words("   ") == ""
        assert normalize_words(None) == ""
        assert normalize_words("A") == "A"
        assert normalize_words("AB") == "AB"

    def test_normalize_words_mixed_cases(self):
        """Don't normalize mixed cases."""
        assert normalize_words("K o d e Wilayah") == "K o d e Wilayah"  # More single chars
        assert normalize_words("Kode W i l a y a h") == "Kode W i l a y a h"  # More single chars

    def test_normalize_words_with_actual_data(self):
        """Test normalizing with actual data."""
        assert normalize_words("D a g h o") == "Dagho"
        assert normalize_words("A l o") == "Alo"
        assert normalize_words("N u n u") == "Nunu"
        assert normalize_words("R a i n i s") == "Rainis"
        assert normalize_words("Limba U I") == "Limba U I"

class TestValidatePageRange:
    """Test cases for the validate_page_range function."""

    def test_validate_page_range_single_pages(self):
        """Test validating single page ranges."""
        assert validate_page_range("1") is True
        assert validate_page_range("1,3,5") is True
        assert validate_page_range("10,20,30") is True

    def test_validate_page_range_ranges(self):
        """Test validating page ranges."""
        assert validate_page_range("1-5") is True
        assert validate_page_range("1-5,7-10") is True
        assert validate_page_range("1,3-5,7,9-12") is True

    def test_validate_page_range_invalid(self):
        """Test validating invalid page ranges."""
        assert validate_page_range("") is False
        assert validate_page_range("a") is False
        assert validate_page_range("1-") is False
        assert validate_page_range("-5") is False
        assert validate_page_range("1,") is False
        assert validate_page_range("1--5") is False


class TestParsePageRange:
    """Test cases for the parse_page_range function."""

    def test_parse_page_range_single_pages(self):
        """Test parsing single page ranges."""
        assert parse_page_range("1", 10) == [1]
        assert parse_page_range("1,3,5", 10) == [1, 3, 5]
        assert parse_page_range("5,1,3", 10) == [1, 3, 5]  # Should be sorted

    def test_parse_page_range_ranges(self):
        """Test parsing page ranges."""
        assert parse_page_range("1-3", 10) == [1, 2, 3]
        assert parse_page_range("1-3,5-7", 10) == [1, 2, 3, 5, 6, 7]

    def test_parse_page_range_with_limits(self):
        """Test parsing page ranges with total page limits."""
        assert parse_page_range("1-15", 10) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        assert parse_page_range("8,9,10,11", 10) == [8, 9, 10]
        assert parse_page_range("0,1,2", 10) == [1, 2]

    def test_parse_page_range_duplicates(self):
        """Test parsing page ranges with duplicates."""
        assert parse_page_range("1,1,2,2", 10) == [1, 2]
        assert parse_page_range("1-3,2-4", 10) == [1, 2, 3, 4]


class TestFormatDuration:
    """Test cases for the format_duration function."""

    def test_format_duration_seconds(self):
        """Test formatting duration in seconds."""
        assert format_duration(1.5) == "1.50s"
        assert format_duration(30.0) == "30.00s"
        assert format_duration(59.99) == "59.99s"

    def test_format_duration_minutes(self):
        """Test formatting duration in minutes and seconds."""
        assert format_duration(60) == "1m 0s"
        assert format_duration(90) == "1m 30s"
        assert format_duration(3599) == "59m 59s"

    def test_format_duration_hours(self):
        """Test formatting duration in hours, minutes, and seconds."""
        assert format_duration(3600) == "1h 0m 0s"
        assert format_duration(3661) == "1h 1m 1s"
        assert format_duration(7323) == "2h 2m 3s"

    def test_format_duration_edge_cases(self):
        """Test formatting duration edge cases."""
        assert format_duration(0) == "0.00s"
        assert format_duration(0.01) == "0.01s"


class TestApplyRegexTransformations:
    """Test cases for the _apply_regex_transformations function."""

    def test_apply_regex_transformations_basic(self):
        """Test basic regex transformations."""
        assert _apply_regex_transformations("ACEH") == "ACEH"
        assert _apply_regex_transformations("SUMATERA UTARA") == "SUMATERA UTARA"

    def test_apply_regex_transformations_digits_newlines(self):
        """Test regex transformations with digits and newlines."""
        assert _apply_regex_transformations("123\nACEH") == "ACEH"
        assert _apply_regex_transformations("ACEH\n456") == "ACEH"

    def test_apply_regex_transformations_multiple_newlines(self):
        """Test regex transformations with multiple newlines."""
        assert _apply_regex_transformations("ACEH\n\n\nBESAR") == "ACEH BESAR"
        assert _apply_regex_transformations("LINE1\n\nLINE2\n\n\nLINE3") == "LINE1 LINE2 LINE3"

    def test_apply_regex_transformations_digits_spaces(self):
        """Test regex transformations with digits and spaces."""
        assert _apply_regex_transformations("123 ACEH") == "ACEH"
        assert _apply_regex_transformations("456 SUMATERA UTARA") == "SUMATERA UTARA"

    def test_apply_regex_transformations_double_spaces(self):
        """Test regex transformations with double spaces."""
        assert _apply_regex_transformations("ACEH  BESAR") == "ACEH BESAR"
        assert _apply_regex_transformations("WORD1   WORD2    WORD3") == "WORD1 WORD2 WORD3"
