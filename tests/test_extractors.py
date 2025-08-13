from pathlib import Path

import pandas as pd
import pytest

from idn_area_etl.extractors import AreaExtractor, IslandExtractor
from idn_area_etl.utils import (
    PROVINCE_CODE_LENGTH,
    REGENCY_CODE_LENGTH,
    DISTRICT_CODE_LENGTH,
    VILLAGE_CODE_LENGTH,
)


# ---------- Helpers to fabricate DataFrames like Camelot ----------
def _df_area():
    """
    Sample DataFrame for area table.
    """
    rows = [
        [
            "K O D E",
            "NAMA PROVINSI / KABUPATEN / KOTA",
            "JUMLAH",
            "",
            "N A M A / J U M L A H",
            "",
            "",
            "LUAS WILAYAH (Km2)",
            "K E T E R A N G A N",
        ],
        ["", "KAB", "KOTA", "KECAMATAN", "KELURAHAN", "D E S A", "", "", ""],
        [
            "11",
            "Aceh",
            "",
            "",
            "",
            "",
            "",
            "",
            "Undang-undang Nomor 11 Tahun 2006 tentang Pemerintahan Aceh",
        ],
        [
            "11.01",
            "Kabupaten Aceh Selatan",
            "18",
            "0",
            "260",
            "",
            "4.174,211",
            "Perbaikan nama ibu kota semula Tapak Tuan menjadi Tapaktuan sesuai Undang-Undang Nomor 12 Tahun 2024 tentang Kabupaten Aceh Selatan di Aceh.",  # noqa: E501
        ],
        ["11.01.01", "1 Bakongan", "-", "7", "", "", "", ""],
        ["11.01.01.2001", "1 Keude Bakongan", "", "", "", "", "", ""],
        [
            "11.01.01.2002",
            "2 Ujong Mangki",
            "",
            "",
            "",
            "",
            "",
            "Perbaikan nama sesuai Surat Pemkab Aceh Selatan No.140/819/2016 tgl 14 okt 2016 dan Rekomendasi Ditjen Bina Pemdes No. 146/3672/BPD tgl 21 Juni 2017",  # noqa: E501
        ],
        [
            "11.01.01.2003",
            "3 Ujong Padang",
            "",
            "",
            "",
            "",
            "",
            "Perbaikan nama sesuai Surat Pemkab Aceh Selatan No.140/819/2016 tgl 14 okt 2016 dan Rekomendasi Ditjen Bina Pemdes No. 146/3672/BPD tgl 21 Juni 2017",  # noqa: E501
        ],
        [
            "11.01.01.2004",
            "4 Gampong Drien",
            "",
            "",
            "",
            "",
            "",
            "Perbaikan nama sesuai Surat Pemkab Aceh Selatan No.140/819/2016 tgl 14 okt 2016 dan Rekomendasi Ditjen Bina Pemdes No. 146/3672/BPD tgl 21 Juni 2017",  # noqa: E501
        ],
        ["", "", "", "", "", "", "Bukit Gadeng", "Menjadi wil Kec. Kota Bahagia Perda No. 3/2010"],
        [
            "",
            "",
            "",
            "",
            "",
            "",
            "Seuneubok Keuranji",
            "Menjadi wil Kec. Kota Bahagia Perda No. 3/2010",
        ],
    ]
    return pd.DataFrame(rows)


def _df_area_unmatched():
    """
    Sample DataFrame for non-area table.
    """
    rows = [
        [
            "NO",
            "KODE",
            "NAMA PROVINSI,\nKABUPATEN / KOTA,\nKECAMATAN",
            "IBUKOTA",
            "J U M L A H",
            "",
            "",
            "",
            "",
            "LUAS \nWILAYAH\n(Km2) *",
            "JUMLAH \nPENDUDUK\n (Jiwa) **",
            "K ET E R A N G A N",
        ],
        ["", "", "", "", "KAB", "KOTA", "KEC", "KEL", "DESA"],
        [
            "I",
            "11",
            "Aceh",
            "Banda Aceh",
            "18",
            "5",
            "290",
            "",
            "6500",
            "56.835",
            "5.623.479",
            "Undang-undang Nomor 11 Tahun 2006 tentang Pemerintahan Aceh",
        ],
        [
            "",
            "11.01",
            "1\nKabupaten Aceh Selatan",
            "Tapaktuan",
            "",
            "",
            "18",
            "0",
            "260",
            "4.174",
            "239.629",
            "Perbaikan nama ibu kota semula Tapak Tuan menjadi Tapaktuan \nsesuai Undang-Undang Nomor 12 Tahun 2024 tentang Kabupaten \nAceh Selatan di Aceh.",  # noqa: E501
        ],
        [
            "",
            "11.01.01",
            "Bakongan\n1",
            "",
            "",
            "",
            "",
            "",
            "7",
            "",
            "",
            "",
        ],
        [
            "",
            "11.01.02",
            "Kluet Utara\n2",
            "",
            "",
            "",
            "",
            "",
            "7",
            "",
            "",
            "",
        ],
        [
            "",
            "11.01.03",
            "Kluet Selatan\n3",
            "",
            "",
            "",
            "",
            "",
            "7",
            "",
            "",
            "",
        ],
    ]
    return pd.DataFrame(rows)


def _df_island():
    """
    Sample DataFrame for island table with banner and split coordinates.
    """
    rows = [
        [
            "Kode Pulau",
            "Nama Provinsi, Kabupaten/Kota, Pulau",
            "Jumlah",
            "Koordinat",
            "Luas\n2\n(Km )",
            "BP/TBP",
            "Keterangan",
        ],
        ["11.01", "Kabupaten Aceh Selatan", "6", "", "", "", ""],
        [
            "11.01.40001",
            "Pulau Batukapal",
            "",
            "03°19'03.44\" U 097°07'41.73\" T",
            "0.0006",
            "TBP",
            "",
        ],
        [
            "11.01.40002",
            "Pulau Batutunggal",
            "",
            "03°24'55.00\" U 097°04'21.00\" T",
            "0.0078",
            "TBP",
            "",
        ],
        [
            "11.01.40004",
            "Pulau Mangki",
            "",
            "02°54'25.11\" U 097°26'18.51\" T",
            "",
            "TBP",
            "",
        ],
        ["11.03", "Kabupaten Aceh Timur", "8", "", "", "", ""],
        [
            "11.03.40003",
            "Pulau Krueng Beukah",
            "",
            "04°36'19.18\" U 098°01'02.04\" T",
            "0.1152",
            "",
            "",
        ],
        [
            "11.03.40005",
            "Pulau Nebukserdang",
            "",
            "05°06'37.00\" U 097°37'35.00\" T",
            "",
            "BP",
            "",
        ],
        [
            "11.06.40007",
            "Pulau Bateeleblah",
            "",
            "05°47'34.72\" U 094°58'26.09\" T",
            "0.0080",
            "TBP",
            "(PPKT)",
        ],
    ]
    return pd.DataFrame(rows)


def _df_island_messy():
    """
    Island table containing a regency-less island (xx.00.xxxxx) and messy coordinates needing
    sanitation.
    """
    rows = [
        [
            "Kode Pulau",
            "Nama Provinsi, Kabupaten/Kota, Pulau",
            "Koordinat",
            "BP/TBP",
            "Keterangan",
        ],
        [
            "12.00.40001",  # regency-less
            "Pulau 1",
            "01°22'40 U 120°53'04 T",
            "BP",
            "(PPKT)",
        ],
        [
            "12.00.40002",  # regency-less
            "Pulau 2",
            "03° 31'33.49\" U 125° 39'37.53\" T",  # internal spaces after degree/minute
            "",
            "(PPKT)",
        ],
        [
            "12.01.40003",
            "Pulau 3",
            '01°18\'47.00"" U 124°30\'46.00"" T',  # duplicate quotes
            "TBP",
            "",
        ],
        [
            "12.01.40004",
            "Pulau 4",
            "01°22'40\" U 120°53'04\" T",  # missing digits
            "",
            "",
        ],
    ]
    return pd.DataFrame(rows)


# ---------- Tests for AreaExtractor ----------
class TestAreaExtractor:
    """Test cases for the AreaExtractor class."""

    def test_matches_true(self, tmp_path: Path):
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        assert ex.matches(_df_area())

    def test_matches_false(self, tmp_path: Path):
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        assert not ex.matches(_df_area_unmatched())
        assert not ex.matches(_df_island())

    def test_matches_empty_dataframe(self, tmp_path: Path):
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        assert not ex.matches(pd.DataFrame())

    def test_matches_insufficient_columns(self, tmp_path: Path):
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        # DataFrame with only 1 column
        df = pd.DataFrame([["K O D E"]])
        assert not ex.matches(df)

    def test_extract_rows_happy_path(self, tmp_path: Path):
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        out = ex.extract_rows(_df_area())

        assert out["province"] == [["11", "Aceh"]]
        assert out["regency"] == [["11.01", "11", "Kabupaten Aceh Selatan"]]
        assert out["district"] == [["11.01.01", "11.01", "Bakongan"]]
        assert out["village"] == [
            ["11.01.01.2001", "11.01.01", "Keude Bakongan"],
            ["11.01.01.2002", "11.01.01", "Ujong Mangki"],
            ["11.01.01.2003", "11.01.01", "Ujong Padang"],
            ["11.01.01.2004", "11.01.01", "Gampong Drien"],
        ]

        # Sanity on length-based classification
        assert len("11") == PROVINCE_CODE_LENGTH
        assert len("11.01") == REGENCY_CODE_LENGTH
        assert len("11.01.02") == DISTRICT_CODE_LENGTH
        assert len("11.01.01.2001") == VILLAGE_CODE_LENGTH

    def test_extract_rows_empty_dataframe(self, tmp_path: Path):
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        out = ex.extract_rows(pd.DataFrame())
        assert out == {"province": [], "regency": [], "district": [], "village": []}

    def test_extract_rows_insufficient_columns(self, tmp_path: Path):
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        # DataFrame with only one column
        df = pd.DataFrame([["K O D E"], ["11"], ["11.01"]])
        out = ex.extract_rows(df)
        assert out == {"province": [], "regency": [], "district": [], "village": []}

    def test_extract_rows_six_column_table(self, tmp_path: Path):
        # Test 6-column table variant (uses columns [1, 3] for names)
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        df_6col = pd.DataFrame(
            [
                ["K O D E", "NAMA", "COL2", "BACKUP_NAME", "COL4", "COL5"],
                ["", "", "", "", "", ""],
                ["11", "Aceh", "", "", "", ""],
                ["11.01", "", "", "Kabupaten Aceh Selatan", "", ""],
            ]
        )
        out = ex.extract_rows(df_6col)
        assert out["province"] == [["11", "Aceh"]]
        assert out["regency"] == [["11.01", "11", "Kabupaten Aceh Selatan"]]

    def test_extract_rows_duplicate_province(self, tmp_path: Path):
        # Test that duplicate provinces are not added twice
        ex = AreaExtractor(destination=tmp_path, output_name="x")
        df_dup = pd.DataFrame(
            [
                ["K O D E", "NAMA", "COL2", "COL3", "COL4", "COL5", "COL6"],
                ["", "", "", "", "", "", ""],
                ["11", "Aceh", "", "", "", "", ""],
                ["11", "Aceh", "", "", "", "", ""],  # Duplicate
            ]
        )
        out = ex.extract_rows(df_dup)
        # Should only have one province entry
        assert len(out["province"]) == 1
        assert out["province"] == [["11", "Aceh"]]


# ---------- Tests for IslandExtractor ----------
class TestIslandExtractor:
    """Test cases for the IslandExtractor class."""

    def test_matches_true(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")
        assert ex.matches(_df_island())

    def test_matches_false(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")
        assert not ex.matches(_df_area())
        assert not ex.matches(_df_area_unmatched())

    def test_extract_empty_dataframe(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")
        out = ex.extract_rows(pd.DataFrame())
        assert out == {"island": []}

    def test_extract_no_header_found(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")
        # DataFrame without island header
        df = pd.DataFrame(
            [
                ["NOT_ISLAND", "DATA"],
                ["11.01", "Something"],
            ]
        )
        out = ex.extract_rows(df)
        assert out == {"island": []}

    def test_extract_header_found_but_no_data(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")
        # DataFrame with header but no data rows
        df = pd.DataFrame(
            [
                ["Kode Pulau", "Nama", "Koordinat"],
            ]
        )
        out = ex.extract_rows(df)
        assert out == {"island": []}

    def test_extract_invalid_island_codes(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")
        # DataFrame with invalid island codes
        df = pd.DataFrame(
            [
                ["Kode Pulau", "Nama"],
                ["INVALID", "Pulau Invalid"],
                ["11.01", "Not island code"],  # Too short
                ["11.01.4000X", "Invalid char"],  # Invalid character
            ]
        )
        out = ex.extract_rows(df)
        assert out == {"island": []}

    def test_extract(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")
        out = ex.extract_rows(_df_island())["island"]
        assert out == [
            [
                "11.01.40001",
                "11.01",
                "03°19'03.44\" N 097°07'41.73\" E",
                "0",
                "0",
                "Pulau Batukapal",
            ],
            [
                "11.01.40002",
                "11.01",
                "03°24'55.00\" N 097°04'21.00\" E",
                "0",
                "0",
                "Pulau Batutunggal",
            ],
            ["11.01.40004", "11.01", "02°54'25.11\" N 097°26'18.51\" E", "0", "0", "Pulau Mangki"],
            [
                "11.03.40003",
                "11.03",
                "04°36'19.18\" N 098°01'02.04\" E",
                "0",
                "0",
                "Pulau Krueng Beukah",
            ],
            [
                "11.03.40005",
                "11.03",
                "05°06'37.00\" N 097°37'35.00\" E",
                "1",
                "0",
                "Pulau Nebukserdang",
            ],
            [
                "11.06.40007",
                "11.06",
                "05°47'34.72\" N 094°58'26.09\" E",
                "0",
                "1",
                "Pulau Bateeleblah",
            ],
        ]

    def test_extract_messy_and_regencyless(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")
        out = ex.extract_rows(_df_island_messy())["island"]
        assert out == [
            ["12.00.40001", "", "01°22'40.00\" N 120°53'04.00\" E", "1", "1", "Pulau 1"],
            ["12.00.40002", "", "03°31'33.49\" N 125°39'37.53\" E", "0", "1", "Pulau 2"],
            ["12.01.40003", "12.01", "01°18'47.00\" N 124°30'46.00\" E", "0", "0", "Pulau 3"],
            ["12.01.40004", "12.01", "01°22'40.00\" N 120°53'04.00\" E", "0", "0", "Pulau 4"],
        ]

    def test_extract_rows_parent_from_code_paths(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")

        df = pd.DataFrame(
            [
                # headers — include 'kode' and 'pulau' to let extractor find columns
                ["no", "kode pulau", "nama pulau", "koordinat", "berpenghuni", "terluar"],
                # valid with regency != '00' -> parent '12.01'
                ["1", "12.01.40003", "Pulau A", "01°18'47.00\" U 124°30'46.00\" T", "0", "0"],
                # valid with regency '00' -> parent None -> empty regency_code
                ["2", "12.00.40001", "Pulau B", "03°31'33.49\" U 125°39'37.53\" T", "0", "1"],
            ]
        )

        out = ex.extract_rows(df)["island"]
        # Row 0: parent should be "12.01"
        assert out[0][0] == "12.01.40003" and out[0][1] == "12.01"
        # Row 1: parent None -> ''
        assert out[1][0] == "12.00.40001" and out[1][1] == ""

    def test_extract_rows_find_name_col_keyword_and_fallback(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")

        # Case A: keyword 'pulau' (non-code) used for "nama" column
        df_a = pd.DataFrame(
            [
                ["no", "kode pulau", "nama pulau", "koordinat"],
                ["1", "12.01.40004", "Pulau X", "01°22'40.00\" U 120°53'04.00\" T"],
            ]
        )
        out_a = ex.extract_rows(df_a)["island"]
        assert out_a and out_a[0][-1] == "Pulau X"

        # Case B: no 'pulau' in headers, fallback to idx_code + 1
        df_b = pd.DataFrame(
            [
                ["no", "kode pulau", "nama sebelah kode", "koordinat"],
                ["1", "12.01.40005", "Pulau Y", "01°22'40.00\" U 120°53'04.00\" T"],
            ]
        )
        out_b = ex.extract_rows(df_b)["island"]
        assert out_b and out_b[0][-1] == "Pulau Y"  # taken from column next to code

    def test_island_extract_rows_name_equals_code_uses_next_col(self, tmp_path: Path):
        """
        Hit the branch:
            if name == code and idx_code is not None and idx_code + 1 < len(r):
                name2 = r[idx_code + 1] ...
                if name2 and name2 != code:
                    name = name2
        """
        ex = IslandExtractor(destination=tmp_path, output_name="x")

        df = pd.DataFrame(
            [
                ["no", "kode pulau", "nama sebelah", "nama pulau", "koordinat"],
                # 'nama pulau' column contains the code (garbage),
                # while 'nama sebelah' (idx_code + 1) contains the real name.
                ["1", "12.01.40003", "Pulau 3", "12.01.40003", "01°18'47.00\" U 124°30'46.00\" T"],
            ]
        )
        out = ex.extract_rows(df)["island"]
        assert out and out[0][0] == "12.01.40003" and out[0][-1] == "Pulau 3"

    def test_extract_rows_uses_next_to_code_when_name_equals_code(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="x")

        # Header order matters:
        # - "kode pulau" at index 1 -> idx_code = 1
        # - "sebelah kode" at index 2 -> this is (idx_code + 1), the fallback source
        # - "nama pulau" at index 3 -> first header containing "nama" => idx_name = 3
        # We will put the CODE string in "nama pulau" to trigger (name == code),
        # and the REAL name in the column right after the code ("sebelah kode").
        df = pd.DataFrame(
            [
                ["no", "kode pulau", "sebelah kode", "nama pulau", "koordinat"],
                ["1", "12.01.40003", "Pulau 3", "12.01.40003", "01°18'47.00\" U 124°30'46.00\" T"],
            ]
        )

        out = ex.extract_rows(df)["island"]
        assert out and out[0][0] == "12.01.40003"
        # Name should be taken from (idx_code + 1) i.e., "sebelah kode"
        assert out[0][-1] == "Pulau 3"
        # (Optional) also ensure coordinate normalized
        assert out[0][2] == "01°18'47.00\" N 124°30'46.00\" E"

    def test_island_extract_rows_returns_empty_when_no_kode_column(self, tmp_path: Path):
        """
        Cover IslandExtractor._find_code_col branch that returns None
        by providing headers without any 'kode' substring.
        Via public API, this should produce no output rows.
        """
        ex = IslandExtractor(destination=tmp_path, output_name="x")

        df = pd.DataFrame(
            [
                ["no", "identifikasi", "nama pulau", "koordinat"],
                ["1", "X123", "Pulau Q", "01°22'40.00\" U 120°53'04.00\" T"],
            ]
        )

        out = ex.extract_rows(df)["island"]
        assert out == []


# ---------- IO behavior via TableExtractor (open/flush/close/write) ----------
class TestTableExtractorIO:
    """Test open_outputs, write_rows, flush, close_outputs via a concrete extractor."""

    def test_write_and_persist_csv(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="sample")
        ex.open_outputs()
        try:
            rows = [
                ["11.01.40001", "11.01", "03°19'03.44\" N 097°07'41.73\" E", "0", "0", "Pulau X"]
            ]
            ex.write_rows("island", rows)
            ex.flush("island")
        finally:
            ex.close_outputs()

        out_path = tmp_path / "sample.island.csv"
        assert out_path.exists()
        content = out_path.read_text(encoding="utf-8").splitlines()
        assert content[0].split(",") == [
            "code",
            "regency_code",
            "coordinate",
            "is_populated",
            "is_outermost_small",
            "name",
        ]
        assert "Pulau X" in content[1]

    def test_write_empty_rows(self, tmp_path: Path):
        ex = IslandExtractor(destination=tmp_path, output_name="sample")
        ex.open_outputs()
        try:
            # Write empty rows should not cause error
            ex.write_rows("island", [])
            ex.flush("island")
        finally:
            ex.close_outputs()

        out_path = tmp_path / "sample.island.csv"
        assert out_path.exists()
        content = out_path.read_text(encoding="utf-8").splitlines()
        # Should only have header
        assert len(content) == 1

    def test_context_manager_exception_handling(self, tmp_path: Path):
        # Test that files are properly closed even when exception occurs
        with IslandExtractor(destination=tmp_path, output_name="sample") as ex:
            rows = [
                ["11.01.40001", "11.01", "03°19'03.44\" N 097°07'41.73\" E", "0", "0", "Pulau X"]
            ]
            ex.write_rows("island", rows)
            # Simulate an exception
            try:
                raise ValueError("Test exception")
            except ValueError:
                pass

        # File should still be properly written and closed
        out_path = tmp_path / "sample.island.csv"
        assert out_path.exists()
        content = out_path.read_text(encoding="utf-8").splitlines()
        assert len(content) == 2  # header + data

    def test_close_outputs_with_file_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        # Test error handling in close_outputs
        ex = IslandExtractor(destination=tmp_path, output_name="sample")
        ex.open_outputs()

        # Mock file close to raise an exception
        def error_close():
            raise OSError("File close error")

        ex.file_handles["island"].close = error_close

        # Should not raise exception even if file close fails
        ex.close_outputs()

        # Verify cleanup still happened
        assert len(ex.file_handles) == 0
        assert len(ex.writers) == 0
        assert len(ex.buffers) == 0

    def test_write_rows_triggers_flush(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        ex = AreaExtractor(destination=tmp_path, output_name="x")

        # Prepare buffer and tiny batch size for a single key, e.g., 'province'
        ex.buffers = {"province": []}
        ex.batch_sizes = {"province": 2}

        calls: list[str] = []

        def fake_flush(k: str) -> None:
            calls.append(k)
            # mimic clearing buffer like real flush would typically do:
            ex.buffers[k].clear()

        monkeypatch.setattr(ex, "flush", fake_flush)

        ex.write_rows("province", [["11", "Aceh"]])  # size=1 -> no flush
        assert calls == []

        ex.write_rows("province", [["12", "Sumatera Utara"]])  # size=2 -> flush happens
        assert calls == ["province"]
