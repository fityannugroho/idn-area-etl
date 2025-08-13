import math
import pytest

from idn_area_etl.utils import (
    PROVINCE_CODE_LENGTH,
    REGENCY_CODE_LENGTH,
    DISTRICT_CODE_LENGTH,
    VILLAGE_CODE_LENGTH,
    RE_ISLAND_CODE,
    clean_name,
    fix_wrapped_name,
    normalize_words,
    format_coordinate,
    chunked,
    validate_page_range,
    parse_page_range,
    format_duration,
)


class TestCleanName:
    """Test cases for the clean_name function."""

    def test_basic_cleaning(self):
        assert clean_name("  Kabupaten   Aceh \tSelatan\r\n") == "Kabupaten Aceh Selatan"

    def test_preserve_internal_newline_semantics(self):
        # Angka header di baris terpisah seharusnya tidak terbawa
        raw = "123\nNama Provinsi\nAceh"
        out = clean_name(raw)
        assert "123" not in out
        assert out == "Nama Provinsi Aceh"

    def test_empty_string(self):
        assert clean_name("") == ""

    def test_whitespace_only_string(self):
        assert clean_name("   \t  \r\n  ") == ""

    def test_complex_number_patterns(self):
        # Test various number patterns at beginning and end
        assert clean_name("123\nSome Name\n456") == "Some Name"
        assert clean_name("1 Some Name") == "Some Name"


class TestFixWrappedName:
    """Test cases for the fix_wrapped_name function."""

    def test_merge_lowercase_tail(self):
        assert fix_wrapped_name("Sibarani Nasampulu/Namungk\nup") == "Sibarani Nasampulu/Namungkup"
        assert fix_wrapped_name("Kedungpomahanwet\nan") == "Kedungpomahanwetan"
        assert fix_wrapped_name("Kedungpomahankulo\nn") == "Kedungpomahankulon"
        assert fix_wrapped_name("Leragawi/Megagiraku\nk") == "Leragawi/Megagirakuk"
        assert fix_wrapped_name("Bakungtemenggunga\nn") == "Bakungtemenggungan"

    def test_keep_regular_breaks(self):
        assert fix_wrapped_name("Pulau Batee\nUjong") == "Pulau Batee\nUjong"
        assert fix_wrapped_name("Ompu Raja Hutapea\nTimur") == "Ompu Raja Hutapea\nTimur"
        assert fix_wrapped_name("Ompu Raja Hutapea\nTimur") != "Ompu Raja HutapeaTimur"
        assert fix_wrapped_name("Teungoh Glumpang\nVII") == "Teungoh Glumpang\nVII"
        assert fix_wrapped_name("Perkebunan Sungai\nIyu") == "Perkebunan Sungai\nIyu"
        assert fix_wrapped_name("Limba U I") == "Limba U I"

    def test_empty_string(self):
        assert fix_wrapped_name("") == ""
        assert fix_wrapped_name("") == ""
        assert fix_wrapped_name("   ") == ""

    def test_no_newlines(self):
        assert fix_wrapped_name("Simple Name") == "Simple Name"

    def test_empty_lines_removal(self):
        assert fix_wrapped_name("Name\n\n\nSecond") == "Name\nSecond"
        assert fix_wrapped_name("Name\n  \n  \nSecond") == "Name\nSecond"

    def test_edge_cases_for_merging(self):
        # Test when previous line is exactly max_line_length
        long_line = "A" * 16  # exactly max_line_length
        assert fix_wrapped_name(f"{long_line}\nup") == f"{long_line}up"

        # Test when fragment is longer than 3 chars (should not merge)
        assert fix_wrapped_name("Short\nlonger") == "Short\nlonger"

        # Test when previous line ends with space or dash (should not merge)
        assert fix_wrapped_name("Line ends with \nup") == "Line ends with\nup"
        assert fix_wrapped_name("Line ends with-\nup") == "Line ends with-\nup"

        # Test when fragment starts with uppercase (should not merge)
        assert fix_wrapped_name("Line\nUp") == "Line\nUp"


class TestNormalizeWords:
    """Test cases for the normalize_words function."""

    def test_join_single_characters(self):
        assert normalize_words("K o d e") == "Kode"

    def test_keep_valid_phrase(self):
        assert normalize_words("Nama Provinsi") == "Nama Provinsi"

    def test_allow_slash_and_dash_tokens(self):
        assert normalize_words("N A M A / P R O V I N S I") == "NAMA/PROVINSI"
        assert normalize_words("A - B") == "A-B"

    def test_blank(self):
        assert normalize_words("   ") == ""


class TestFormatCoordinate:
    """Test cases for the format_coordinate function."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            # Baseline: already canonical
            ("03°19'03.44\" N 097°07'41.73\" E", "03°19'03.44\" N 097°07'41.73\" E"),
            # Indonesian hemispheres → N/E
            ("03°19'03.44\" U 097°07'41.73\" T", "03°19'03.44\" N 097°07'41.73\" E"),
            ("03°19'03.44\" LU 097°07'41.73\" BT", "03°19'03.44\" N 097°07'41.73\" E"),
            # Whitespace
            ("03° 31'33.49\"  U   125° 39'37.53\"   T", "03°31'33.49\" N 125°39'37.53\" E"),
            # S/W mapping + seconds padding to 2 decimals
            ("03°19'03.4\" S 097°07'41.7\" B", "03°19'03.40\" S 097°07'41.70\" W"),
            # Duplicate quotes
            ('01°18\'47.00"" U 124°30\'46.00"" T', "01°18'47.00\" N 124°30'46.00\" E"),
            # Seconds truncation to 2 decimals
            ("03°19'03.444\" N 097°07'41.735\" E", "03°19'03.44\" N 097°07'41.73\" E"),
            # Missing seconds quote → should add `"`.
            ("03°19'03.44 N 097°07'41.73 E", "03°19'03.44\" N 097°07'41.73\" E"),
            # Smart quotes / double prime should normalize
            ("03°19’03.44″ LU 097°07’41.73″ BT", "03°19'03.44\" N 097°07'41.73\" E"),
            # Handle pattern: LAT first, LON second, then another LAT.
            ("03°19'03.44\" N 097°07'41.73\" E 00°00'00\" N", "03°19'03.44\" N 097°07'41.73\" E"),
            # # Opposite order: LON first, LAT second, then another LON.
            ("097°07'41.73\" E 03°19'03.44\" N 000°00'00\" E", "03°19'03.44\" N 097°07'41.73\" E"),
            # ------------------------
            #      Negative tests
            # ------------------------
            # Hemisphere in front of token
            ("N 03°19'03.44\" E 097°07'41.73\"", "N 03°19'03.44\" E 097°07'41.73\""),
            # hemisphere leading, DMS invalid → hit fallback "leading hemi"
            ("N 03 19 03.4 E 097 07 41.7", "N 03 19 03.4 E 097 07 41.7"),
            # hemisphere trailing but odd spacing, DMS invalid → hit fallback "trailing hemi odd space"  # noqa: E501
            ("03 19 03.4   N   097 07 41.7   E", "03 19 03.4 N 097 07 41.7 E"),
            # Fully unparseable but with hemisphere tokens → fallback normalize only
            ("U T", "N E"),
            # Fully unparseable with no hemisphere tokens → fallback returns input (trim/spaces per impl)  # noqa: E501
            ("abc", "abc"),
            # Weird data: cannot be parsed into two DMS -> returned after hemisphere normalization
            ("Invalid coordinate", "Invalid coordinate"),
        ],
    )
    def test_format_coordinate_covers_all_branches(self, raw: str, expected: str) -> None:
        assert format_coordinate(raw) == expected

    @pytest.mark.parametrize("raw", ["", "   ", "\t", "\n"])
    def test_format_coordinate_empty_input(self, raw: str) -> None:
        assert format_coordinate(raw) == ""


@pytest.mark.parametrize("raw", ["", "   "])
def test_format_coordinate_empty_input(raw: str) -> None:
    assert format_coordinate(raw) == ""


class TestChunked:
    """Test cases for the chunked function."""

    def test_regular_chunks(self):
        data = list(range(1, 10))
        assert list(chunked(data, 4)) == [[1, 2, 3, 4], [5, 6, 7, 8], [9]]

    def test_zero_size_raises(self):
        with pytest.raises(ValueError):
            list(chunked([1, 2, 3], 0))

    def test_negative_size_gives_empty(self):
        # range step negatif menghasilkan iterator kosong pada implementasi saat ini
        assert list(chunked([1, 2, 3], -2)) == []

    def test_empty_list(self):
        assert list(chunked([], 3)) == []

    def test_single_element(self):
        assert list(chunked([1], 3)) == [[1]]

    def test_chunk_size_larger_than_data(self):
        assert list(chunked([1, 2], 5)) == [[1, 2]]

    def test_chunk_size_one(self):
        assert list(chunked([1, 2, 3], 1)) == [[1], [2], [3]]


class TestValidateAndParsePageRange:
    """Test cases for page range helpers."""

    def test_validate_page_range_positive(self):
        assert validate_page_range("1,3,5-7,10")
        assert validate_page_range("2-2")

    def test_validate_page_range_negative(self):
        assert not validate_page_range("1,,3")
        assert not validate_page_range("a-b")
        assert not validate_page_range("")

    def test_parse_page_range_positive(self):
        assert parse_page_range("1", 10) == [1]
        assert parse_page_range("1,3,5", 10) == [1, 3, 5]
        assert parse_page_range("1-3", 10) == [1, 2, 3]
        assert parse_page_range("1-15", 10) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        assert parse_page_range("1-3,2,5-6,100", 10) == [1, 2, 3, 5, 6]

    def test_parse_page_range_negative_values_raise(self):
        with pytest.raises(ValueError):
            parse_page_range("a-b", total_pages=10)


class TestFormatDuration:
    """Test cases for the format_duration function."""

    def test_format_hours_minutes_seconds(self):
        assert format_duration(3723.4) == "1h 2m 3s"

    def test_format_minutes_seconds(self):
        assert format_duration(125.0) == "2m 5s"

    def test_format_seconds(self):
        out = format_duration(3.5)
        assert out.endswith("s")
        assert math.isclose(float(out[:-1]), 3.50, rel_tol=1e-3)


class TestConstants:
    """Sanity checks for exported constants."""

    def test_code_length_constants(self):
        assert PROVINCE_CODE_LENGTH == 2
        assert REGENCY_CODE_LENGTH == 5
        assert DISTRICT_CODE_LENGTH == 8
        assert VILLAGE_CODE_LENGTH == 13

    def test_island_code_regex_positive(self):
        assert RE_ISLAND_CODE.match("11.01.40001")

    def test_island_code_regex_negative(self):
        assert not RE_ISLAND_CODE.match("bad.code")
