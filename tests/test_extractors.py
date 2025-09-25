import csv
from pathlib import Path

import pandas as pd
import pytest

from idn_area_etl.utils import (
    PROVINCE_CODE_LENGTH,
    REGENCY_CODE_LENGTH,
    DISTRICT_CODE_LENGTH,
    VILLAGE_CODE_LENGTH,
)
from idn_area_etl.config import Config, DataConfig
from idn_area_etl.extractors import AreaExtractor, IslandExtractor
from idn_area_etl.writer import OutputWriter


# ---------- Test config fixture ----------


@pytest.fixture()
def config() -> Config:
    """Provide a minimal configuration compatible with the production extractors."""

    return Config(
        data={
            "province": DataConfig(
                batch_size=2,
                output_headers=("code", "name"),
                filename_suffix="province",
            ),
            "regency": DataConfig(
                batch_size=2,
                output_headers=("code", "province_code", "name"),
                filename_suffix="regency",
            ),
            "district": DataConfig(
                batch_size=2,
                output_headers=("code", "regency_code", "name"),
                filename_suffix="district",
            ),
            "village": DataConfig(
                batch_size=2,
                output_headers=("code", "district_code", "name"),
                filename_suffix="village",
            ),
            "island": DataConfig(
                batch_size=2,
                output_headers=(
                    "code",
                    "regency_code",
                    "coordinate",
                    "is_populated",
                    "is_outermost_small",
                    "name",
                ),
                filename_suffix="island",
            ),
        }
    )


def _area_extractor(tmp_path: Path, config: Config, output_name: str = "x") -> AreaExtractor:
    return AreaExtractor(destination=tmp_path, output_name=output_name, config=config)


def _island_extractor(tmp_path: Path, config: Config, output_name: str = "x") -> IslandExtractor:
    return IslandExtractor(destination=tmp_path, output_name=output_name, config=config)


def _read_rows(tmp_path: Path, output_name: str, suffix: str) -> list[list[str]]:
    path = tmp_path / f"{output_name}.{suffix}.csv"
    if not path.exists():
        return []

    with path.open(newline="", encoding="utf-8") as fp:
        rows = list(csv.reader(fp))

    return rows[1:] if len(rows) > 1 else []


def _run_area_extraction(
    df: pd.DataFrame, tmp_path: Path, config: Config, *, output_name: str = "x"
) -> tuple[int, dict[str, list[list[str]]]]:
    with _area_extractor(tmp_path, config, output_name) as ex:
        count = ex.extract_and_write(df)

    outputs = {
        "province": _read_rows(tmp_path, output_name, "province"),
        "regency": _read_rows(tmp_path, output_name, "regency"),
        "district": _read_rows(tmp_path, output_name, "district"),
        "village": _read_rows(tmp_path, output_name, "village"),
    }

    return count, outputs


def _run_island_extraction(
    df: pd.DataFrame, tmp_path: Path, config: Config, *, output_name: str = "x"
) -> tuple[int, dict[str, list[list[str]]]]:
    with _island_extractor(tmp_path, config, output_name) as ex:
        count = ex.extract_and_write(df)

    return count, {"island": _read_rows(tmp_path, output_name, "island")}


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

    def test_matches_true(self, tmp_path: Path, config: Config):
        ex = _area_extractor(tmp_path, config)
        assert ex.matches(_df_area())

    def test_matches_false(self, tmp_path: Path, config: Config):
        ex = _area_extractor(tmp_path, config)
        assert not ex.matches(_df_area_unmatched())
        assert not ex.matches(_df_island())

    def test_matches_empty_dataframe(self, tmp_path: Path, config: Config):
        ex = _area_extractor(tmp_path, config)
        assert not ex.matches(pd.DataFrame())

    def test_matches_insufficient_columns(self, tmp_path: Path, config: Config):
        ex = _area_extractor(tmp_path, config)
        # DataFrame with only 1 column
        df = pd.DataFrame([["K O D E"]])
        assert not ex.matches(df)

    def test_extract_rows_happy_path(self, tmp_path: Path, config: Config):
        count, outputs = _run_area_extraction(_df_area(), tmp_path, config)

        assert count == 7
        assert outputs["province"] == [["11", "Aceh"]]
        assert outputs["regency"] == [["11.01", "11", "Kabupaten Aceh Selatan"]]
        assert outputs["district"] == [["11.01.01", "11.01", "Bakongan"]]
        assert outputs["village"] == [
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

    def test_extract_rows_empty_dataframe(self, tmp_path: Path, config: Config):
        count, outputs = _run_area_extraction(pd.DataFrame(), tmp_path, config)
        assert count == 0
        assert outputs == {"province": [], "regency": [], "district": [], "village": []}

    def test_extract_rows_insufficient_columns(self, tmp_path: Path, config: Config):
        # DataFrame with only one column
        df = pd.DataFrame([["K O D E"], ["11"], ["11.01"]])
        count, outputs = _run_area_extraction(df, tmp_path, config)
        assert count == 0
        assert outputs == {"province": [], "regency": [], "district": [], "village": []}

    def test_extract_rows_six_column_table(self, tmp_path: Path, config: Config):
        # Test 6-column table variant (uses columns [1, 3] for names)
        df_6col = pd.DataFrame(
            [
                ["K O D E", "NAMA", "COL2", "BACKUP_NAME", "COL4", "COL5"],
                ["", "", "", "", "", ""],
                ["11", "Aceh", "", "", "", ""],
                ["11.01", "", "", "Kabupaten Aceh Selatan", "", ""],
            ]
        )
        count, outputs = _run_area_extraction(df_6col, tmp_path, config)
        assert count == 2
        assert outputs["province"] == [["11", "Aceh"]]
        assert outputs["regency"] == [["11.01", "11", "Kabupaten Aceh Selatan"]]

    def test_extract_rows_duplicate_province(self, tmp_path: Path, config: Config):
        # Test that duplicate provinces are not added twice
        df_dup = pd.DataFrame(
            [
                ["K O D E", "NAMA", "COL2", "COL3", "COL4", "COL5", "COL6"],
                ["", "", "", "", "", "", ""],
                ["11", "Aceh", "", "", "", "", ""],
                ["11", "Aceh", "", "", "", "", ""],  # Duplicate
            ]
        )
        count, outputs = _run_area_extraction(df_dup, tmp_path, config)
        assert count == 1
        assert outputs["province"] == [["11", "Aceh"]]


# ---------- Tests for IslandExtractor ----------
class TestIslandExtractor:
    """Test cases for the IslandExtractor class."""

    def test_matches_true(self, tmp_path: Path, config: Config):
        ex = _island_extractor(tmp_path, config)
        assert ex.matches(_df_island())

    def test_matches_false(self, tmp_path: Path, config: Config):
        ex = _island_extractor(tmp_path, config)
        assert not ex.matches(_df_area())
        assert not ex.matches(_df_area_unmatched())

    def test_extract_empty_dataframe(self, tmp_path: Path, config: Config):
        count, outputs = _run_island_extraction(pd.DataFrame(), tmp_path, config)
        assert count == 0
        assert outputs == {"island": []}

    def test_extract_no_header_found(self, tmp_path: Path, config: Config):
        # DataFrame without island header
        df = pd.DataFrame(
            [
                ["NOT_ISLAND", "DATA"],
                ["11.01", "Something"],
            ]
        )
        count, outputs = _run_island_extraction(df, tmp_path, config)
        assert count == 0
        assert outputs == {"island": []}

    def test_extract_header_found_but_no_data(self, tmp_path: Path, config: Config):
        # DataFrame with header but no data rows
        df = pd.DataFrame(
            [
                ["Kode Pulau", "Nama", "Koordinat"],
            ]
        )
        count, outputs = _run_island_extraction(df, tmp_path, config)
        assert count == 0
        assert outputs == {"island": []}

    def test_extract_invalid_island_codes(self, tmp_path: Path, config: Config):
        # DataFrame with invalid island codes
        df = pd.DataFrame(
            [
                ["Kode Pulau", "Nama"],
                ["INVALID", "Pulau Invalid"],
                ["11.01", "Not island code"],  # Too short
                ["11.01.4000X", "Invalid char"],  # Invalid character
            ]
        )
        count, outputs = _run_island_extraction(df, tmp_path, config)
        assert count == 0
        assert outputs == {"island": []}

    def test_extract(self, tmp_path: Path, config: Config):
        count, outputs = _run_island_extraction(_df_island(), tmp_path, config)
        expected = [
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
        assert count == len(expected)
        assert outputs["island"] == expected

    def test_extract_messy_and_regencyless(self, tmp_path: Path, config: Config):
        count, outputs = _run_island_extraction(_df_island_messy(), tmp_path, config)
        expected = [
            ["12.00.40001", "", "01°22'40.00\" N 120°53'04.00\" E", "1", "1", "Pulau 1"],
            ["12.00.40002", "", "03°31'33.49\" N 125°39'37.53\" E", "0", "1", "Pulau 2"],
            ["12.01.40003", "12.01", "01°18'47.00\" N 124°30'46.00\" E", "0", "0", "Pulau 3"],
            ["12.01.40004", "12.01", "01°22'40.00\" N 120°53'04.00\" E", "0", "0", "Pulau 4"],
        ]
        assert count == len(expected)
        assert outputs["island"] == expected

    def test_extract_rows_parent_from_code_paths(self, tmp_path: Path, config: Config):
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

        count, outputs = _run_island_extraction(df, tmp_path, config)
        out = outputs["island"]
        # Row 0: parent should be "12.01"
        assert out[0][0] == "12.01.40003" and out[0][1] == "12.01"
        # Row 1: parent None -> ''
        assert out[1][0] == "12.00.40001" and out[1][1] == ""
        assert count == 2

    def test_extract_rows_find_name_col_keyword_and_fallback(self, tmp_path: Path, config: Config):
        # Case A: keyword 'pulau' (non-code) used for "nama" column
        df_a = pd.DataFrame(
            [
                ["no", "kode pulau", "nama pulau", "koordinat"],
                ["1", "12.01.40004", "Pulau X", "01°22'40.00\" U 120°53'04.00\" T"],
            ]
        )
        _, outputs_a = _run_island_extraction(df_a, tmp_path, config, output_name="case_a")
        out_a = outputs_a["island"]
        assert out_a and out_a[0][-1] == "Pulau X"

        # Case B: no 'pulau' in headers, fallback to idx_code + 1
        df_b = pd.DataFrame(
            [
                ["no", "kode pulau", "nama sebelah kode", "koordinat"],
                ["1", "12.01.40005", "Pulau Y", "01°22'40.00\" U 120°53'04.00\" T"],
            ]
        )
        _, outputs_b = _run_island_extraction(df_b, tmp_path, config, output_name="case_b")
        out_b = outputs_b["island"]
        assert out_b and out_b[0][-1] == "Pulau Y"  # taken from column next to code

    def test_island_extract_rows_name_equals_code_uses_next_col(
        self, tmp_path: Path, config: Config
    ):
        """
        Hit the branch:
            if name == code and idx_code is not None and idx_code + 1 < len(r):
                name2 = r[idx_code + 1] ...
                if name2 and name2 != code:
                    name = name2
        """
        df = pd.DataFrame(
            [
                ["no", "kode pulau", "nama sebelah", "nama pulau", "koordinat"],
                # 'nama pulau' column contains the code (garbage),
                # while 'nama sebelah' (idx_code + 1) contains the real name.
                ["1", "12.01.40003", "Pulau 3", "12.01.40003", "01°18'47.00\" U 124°30'46.00\" T"],
            ]
        )
        _, outputs = _run_island_extraction(df, tmp_path, config, output_name="case_c")
        out = outputs["island"]
        assert out and out[0][0] == "12.01.40003" and out[0][-1] == "Pulau 3"

    def test_extract_rows_uses_next_to_code_when_name_equals_code(
        self, tmp_path: Path, config: Config
    ):
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
        _, outputs = _run_island_extraction(df, tmp_path, config, output_name="case_d")
        out = outputs["island"]
        assert out and out[0][0] == "12.01.40003"
        # Name should be taken from (idx_code + 1) i.e., "sebelah kode"
        assert out[0][-1] == "Pulau 3"
        # (Optional) also ensure coordinate normalized
        assert out[0][2] == "01°18'47.00\" N 124°30'46.00\" E"

    def test_island_extract_rows_returns_empty_when_no_kode_column(
        self, tmp_path: Path, config: Config
    ):
        """
        Cover IslandExtractor._find_code_col branch that returns None
        by providing headers without any 'kode' substring.
        Via public API, this should produce no output rows.
        """
        df = pd.DataFrame(
            [
                ["no", "identifikasi", "nama pulau", "koordinat"],
                ["1", "X123", "Pulau Q", "01°22'40.00\" U 120°53'04.00\" T"],
            ]
        )

        count, outputs = _run_island_extraction(df, tmp_path, config)
        assert count == 0
        assert outputs["island"] == []


# ---------- IO behavior via TableExtractor (public API) ----------
class TestTableExtractorIO:
    """Exercise extractor IO flows using only the public interface."""

    def test_extract_and_persist_csv(self, tmp_path: Path, config: Config) -> None:
        df = pd.DataFrame(
            [
                ["header", "kode pulau", "nama", "koordinat", "status"],
                ["1", "12.01.40001", "Pulau X", "03°19'03.44\" U 097°07'41.73\" T", "BP"],
            ]
        )

        count, outputs = _run_island_extraction(df, tmp_path, config, output_name="sample")
        assert count == 1
        assert outputs["island"] == [
            ["12.01.40001", "12.01", "03°19'03.44\" N 097°07'41.73\" E", "1", "0", "Pulau X"]
        ]

        out_path = tmp_path / "sample.island.csv"
        content = out_path.read_text(encoding="utf-8").splitlines()
        assert content[0].startswith("code,regency_code")

    def test_extract_and_write_empty_result(self, tmp_path: Path, config: Config) -> None:
        count, outputs = _run_island_extraction(
            pd.DataFrame(), tmp_path, config, output_name="empty"
        )
        assert count == 0
        assert outputs["island"] == []

        out_path = tmp_path / "empty.island.csv"
        lines = out_path.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1  # header only

    def test_context_manager_exception_handling(self, tmp_path: Path, config: Config) -> None:
        df = pd.DataFrame(
            [
                ["header", "kode pulau", "nama", "koordinat", "status"],
                ["1", "12.01.40001", "Pulau X", "03°19'03.44\" U 097°07'41.73\" T", "BP"],
            ]
        )

        with pytest.raises(ValueError):
            with _island_extractor(tmp_path, config, output_name="ctx") as ex:
                ex.extract_and_write(df)
                raise ValueError("Test exception")

        out_path = tmp_path / "ctx.island.csv"
        assert out_path.exists()
        rows = list(csv.reader(out_path.open(newline="", encoding="utf-8")))
        assert len(rows) == 2

    def test_extract_and_write_propagates_close_errors(
        self, tmp_path: Path, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def error_close(self: OutputWriter) -> None:
            raise OSError("File close error")

        monkeypatch.setattr(OutputWriter, "close", error_close)

        df = pd.DataFrame(
            [
                ["header", "kode pulau", "nama", "koordinat", "status"],
                ["1", "12.01.40001", "Pulau X", "03°19'03.44\" U 097°07'41.73\" T", "BP"],
            ]
        )

        with pytest.raises(OSError):
            with _island_extractor(tmp_path, config, output_name="err") as ex:
                ex.extract_and_write(df)

    def test_repeated_extract_and_write_appends_rows(self, tmp_path: Path, config: Config) -> None:
        df_province = pd.DataFrame(
            [
                ["K O D E", "NAMA", "COL2", "COL3", "ALT1", "ALT2", "ALT3"],
                ["", "", "", "", "", "", ""],
                ["11", "Aceh", "", "", "", "", ""],
            ]
        )

        df_regency = pd.DataFrame(
            [
                ["K O D E", "NAMA", "COL2", "COL3", "ALT1", "ALT2", "ALT3"],
                ["", "", "", "", "", "", ""],
                ["11.02", "", "", "", "Kabupaten Aceh Barat", "", ""],
            ]
        )

        with _area_extractor(tmp_path, config, output_name="multi") as ex:
            ex.extract_and_write(df_province)
            ex.extract_and_write(df_regency)

        province_rows = _read_rows(tmp_path, "multi", "province")
        regency_rows = _read_rows(tmp_path, "multi", "regency")

        assert province_rows == [["11", "Aceh"]]
        assert regency_rows == [["11.02", "11", "Kabupaten Aceh Barat"]]
