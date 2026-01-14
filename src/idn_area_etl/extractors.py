import re
from abc import ABC, abstractmethod
from pathlib import Path
from types import TracebackType
from typing import Callable, Self

import pandas as pd

from idn_area_etl.config import Area, Config
from idn_area_etl.utils import (
    DISTRICT_CODE_LENGTH,
    PROVINCE_CODE_LENGTH,
    RE_ISLAND_CODE,
    REGENCY_CODE_LENGTH,
    VILLAGE_CODE_LENGTH,
    clean_name,
    fix_wrapped_name,
    format_coordinate,
    is_fuzzy_contained,
    is_fuzzy_match,
    normalize_words,
)
from idn_area_etl.writer import OutputWriter


class TableExtractor(ABC):
    """
    Base extractor that:
     - decides matching tables (matches)
     - extracts ready rows (extract_rows)
     - writes rows to its own CSV target(s) with buffering
    """

    areas: frozenset[Area]
    """Define which areas this extractor handles."""

    def __init__(self, destination: Path, output_name: str, config: Config) -> None:
        self.destination = destination
        self.output_name = output_name
        self.config = config

        # resources for output management
        self._writers: dict[Area, OutputWriter] = {
            area: OutputWriter(
                self.destination / f"{self.output_name}.{config.data[area].filename_suffix}.csv",
                header=self.config.data[area].output_headers,
            )
            for area in self.areas
        }

    def __enter__(self) -> Self:
        self._open_outputs()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._close_outputs()

    def _open_outputs(self) -> None:
        for writer in self._writers.values():
            writer.open()

    def _close_outputs(self) -> None:
        for writer in self._writers.values():
            writer.flush()
            writer.close()

    def _write_rows(self, area: Area, rows: list[list[str]]) -> None:
        if not rows:
            return

        self._writers[area].add(rows)

        if len(self._writers[area]) >= self.config.data[area].batch_size:
            self._writers[area].flush()

    @abstractmethod
    def matches(self, df: pd.DataFrame) -> bool:
        """Return True if this extractor can handle the given table."""
        ...

    @abstractmethod
    def _extract_rows(self, df: pd.DataFrame) -> dict[Area, list[list[str]]]:
        """
        Extract rows from the given DataFrame, grouped by area key.
        Return a dict mapping area key -> list of rows (as list of strings).
        """
        ...

    def extract_and_write(self, df: pd.DataFrame) -> int:
        """Extract rows from the given DataFrame and write them to output files."""

        data = self._extract_rows(df)
        total = 0
        for key, rows in data.items():
            self._write_rows(key, rows)
            total += len(rows)
        return total


class AreaExtractor(TableExtractor):
    """
    Handle four outputs at once: province/regency/district/village.
    """

    areas = frozenset({"province", "regency", "district", "village"})

    def __init__(self, destination: Path, output_name: str, config: Config) -> None:
        super().__init__(destination, output_name, config=config)
        self._seen_provinces: set[str] = set()

        # Get extractor config from extractors.area
        self._extractor_config = config.extractors.area
        self._fuzzy_threshold = config.fuzzy_threshold
        self._exclude_threshold = config.exclude_threshold

    def matches(self, df: pd.DataFrame) -> bool:
        if df.empty or df.shape[0] < 3:  # Need at least 3 rows to check headers
            return False

        # Check rows 0-2 for headers (like IslandExtractor)
        for i in range(min(3, len(df))):
            normalized_headers = [normalize_words(str(col)).lower() for col in df.iloc[i]]

            if len(normalized_headers) < 2:
                continue

            # Check for exclusion keywords first (fast rejection)
            if self._has_exclusion_keywords(normalized_headers):
                return False

            # Check for 'Kode' column
            has_kode = is_fuzzy_match(normalized_headers[0], "kode", self._fuzzy_threshold)

            # Check for 'Nama' or any area-type keyword in any subsequent column
            # This is more flexible than requiring "nama provinsi" specifically
            area_keywords = [
                "nama",
                "provinsi",
                "kabupaten",
                "kota",
                "kecamatan",
                "desa",
                "kelurahan",
            ]
            has_nama = any(
                any(
                    is_fuzzy_contained(keyword, h, self._fuzzy_threshold)
                    for keyword in area_keywords
                )
                for h in normalized_headers[1:]
            )

            if has_kode and has_nama:
                return True

        return False

    def _has_exclusion_keywords(self, headers: list[str]) -> bool:
        """Check if headers contain exclusion keywords."""
        # Join headers for multi-word matching
        joined = " ".join(headers)

        for keyword in self._extractor_config.exclude_keywords:
            # Exact match for short keywords like "NO"
            if keyword == "no":
                for header in headers:
                    if header == "no":
                        return True
            # Fuzzy match for other keywords
            elif is_fuzzy_contained(keyword, joined, self._exclude_threshold):
                return True

        return False

    def _find_columns_by_keywords(
        self,
        headers: list[str],
        keywords: tuple[str, ...],
    ) -> list[int]:
        """
        Find all column indices that match given keywords above threshold.

        Returns list of matching column indices, excluding column 0 (code column).
        Columns are sorted by match quality (best score first, then longest keyword).
        """
        from rapidfuzz import fuzz

        matches: list[tuple[int, float, int, float]] = []  # (col_idx, score, keyword_len, ratio)

        # Score each column (except 0) against all keywords
        for idx in range(1, len(headers)):
            header = headers[idx].lower()
            best_score_for_col = 0.0
            best_kw_len_for_col = 0
            best_ratio_for_col = 0.0

            for keyword in keywords:
                keyword_lower = keyword.lower()

                # Calculate full ratio for tie-breaking
                ratio = fuzz.ratio(keyword_lower, header)

                # Try exact substring match first
                if keyword_lower in header:
                    score = 100.0
                else:
                    # Use partial_ratio for fuzzy matching
                    if len(keyword_lower) > len(header):
                        score = ratio
                    else:
                        score = fuzz.partial_ratio(keyword_lower, header)

                # Track best score and keyword length for this column
                if score >= self._fuzzy_threshold:
                    if score > best_score_for_col:
                        best_score_for_col = score
                        best_kw_len_for_col = len(keyword_lower)
                        best_ratio_for_col = ratio
                    elif score == best_score_for_col:
                        if len(keyword_lower) > best_kw_len_for_col:
                            best_kw_len_for_col = len(keyword_lower)
                            best_ratio_for_col = ratio
                        elif len(keyword_lower) == best_kw_len_for_col:
                            if ratio > best_ratio_for_col:
                                best_ratio_for_col = ratio

            if best_score_for_col >= self._fuzzy_threshold:
                matches.append((idx, best_score_for_col, best_kw_len_for_col, best_ratio_for_col))

        # Sort by score (descending), then keyword length (descending), then ratio (descending)
        matches.sort(key=lambda x: (x[1], x[2], x[3]), reverse=True)

        return [idx for idx, _, _, _ in matches]

    def _find_header_row(self, df: pd.DataFrame) -> int | None:
        """
        Find which row contains area table headers.

        Looks for rows with:
        - "kode" in first column
        - "nama" or any area-type keyword in subsequent columns

        Returns:
            Row index (0-2) or None if not found
        """
        if df.empty or df.shape[0] < 1:
            return None

        for row_idx in range(min(3, len(df))):
            normalized_headers = [normalize_words(str(col)).lower() for col in df.iloc[row_idx]]

            if len(normalized_headers) < 2:
                continue

            # Check for 'Kode' column in position 0
            has_kode = is_fuzzy_match(normalized_headers[0], "kode", self._fuzzy_threshold)

            # Check for 'Nama' or any area-type keyword in any subsequent column
            # This is more flexible than requiring "nama provinsi" specifically
            area_keywords = [
                "nama",
                "provinsi",
                "kabupaten",
                "kota",
                "kecamatan",
                "desa",
                "kelurahan",
            ]
            has_nama = any(
                any(
                    is_fuzzy_contained(keyword, h, self._fuzzy_threshold)
                    for keyword in area_keywords
                )
                for h in normalized_headers[1:]
            )

            if has_kode and has_nama:
                return row_idx

        return None

    def _is_sub_header_row(self, row: pd.Series) -> bool:
        """
        Check if row is a sub-header (e.g., 'KAB', 'KOTA', 'KECAMATAN') vs data row.

        Sub-header characteristics:
        - Column 0 is empty or non-code
        - Contains area type keywords (kabupaten, kota, kecamatan, desa, kelurahan)
        - No actual area codes (no patterns like "11.01")
        - Short text cells (not long descriptions)

        Returns:
            True if row is a sub-header, False if data
        """
        # Check column 0 - should be empty or non-code for sub-header
        col0 = str(row.iloc[0]).strip()
        if col0:
            # If column 0 has content, check if it's an area code
            if re.match(r"^\d+(\.\d+)*$", col0):
                return False  # Data row with code

        # Normalize all cells
        normalized = [normalize_words(str(col)).lower() for col in row]

        # Check for sub-header keywords
        sub_header_keywords = ["kab", "kabupaten", "kota", "kecamatan", "kelurahan", "desa"]
        has_keywords = any(
            any(keyword in cell for keyword in sub_header_keywords) for cell in normalized if cell
        )

        if not has_keywords:
            return False

        # Validate it doesn't look like data (no long text)
        has_long_text = any(len(str(cell).strip()) > 30 for cell in row)

        return not has_long_text

    def _infer_column_map(self, df: pd.DataFrame) -> dict[str, dict[str, list[int] | int]]:
        """
        Identify code and name columns for each area type using fuzzy matching.

        Uses validated header detection to prevent data row leakage.

        Returns:
            {
                "province": {"code": 0, "name": [idx1, idx2, ...]},
                "regency": {"code": 0, "name": [idx1, idx2, ...]},
                "district": {"code": 0, "name": [idx1, idx2, ...]},
                "village": {"code": 0, "name": [idx1, idx2, ...]},
            }

            Where "code" is always int and "name" is always list[int].
        """
        if df.empty or df.shape[0] < 1:
            # Fallback
            return {
                "province": {"code": 0, "name": [1]},
                "regency": {"code": 0, "name": [1]},
                "district": {"code": 0, "name": [1]},
                "village": {"code": 0, "name": [1]},
            }

        # STEP 1: Find the actual header row
        header_idx = self._find_header_row(df)
        if header_idx is None:
            # Fallback: use row 0 as header
            header_idx = 0

        # STEP 2: Get main header
        all_headers = [normalize_words(str(col)).lower() for col in df.iloc[header_idx]]

        # STEP 3: Check if next row is sub-header and merge if so
        if header_idx + 1 < len(df) and self._is_sub_header_row(df.iloc[header_idx + 1]):
            sub_headers = [normalize_words(str(col)).lower() for col in df.iloc[header_idx + 1]]
            # Merge: "nama provinsi" + "kabupaten" = "nama provinsi kabupaten"
            all_headers = [
                f"{all_headers[i]} {sub_headers[i]}".strip()
                if i < len(sub_headers)
                else all_headers[i]
                for i in range(len(all_headers))
            ]

        # For each area type, find name columns using type-specific keywords
        result: dict[str, dict[str, list[int] | int]] = {}

        for area_type in ["province", "regency", "district", "village"]:
            area_config = getattr(self._extractor_config, area_type)

            # Code column: use type-specific if provided, otherwise use shared
            code_keywords = (
                area_config.code_keywords
                if area_config.code_keywords
                else self._extractor_config.code_keywords
            )

            # For code column, check column 0 first (most common case)
            code_idx = 0  # Default to column 0
            if code_keywords:
                # Verify column 0 matches code keywords
                if not any(
                    is_fuzzy_contained(kw, all_headers[0], self._fuzzy_threshold)
                    for kw in code_keywords
                ):
                    # If column 0 doesn't match, search for it
                    for idx in range(len(all_headers)):
                        if any(
                            is_fuzzy_contained(kw, all_headers[idx], self._fuzzy_threshold)
                            for kw in code_keywords
                        ):
                            code_idx = idx
                            break

            # Name columns: use type-specific keywords, return all matches
            name_indices = self._find_columns_by_keywords(all_headers, area_config.name_keywords)
            if not name_indices:
                name_indices = [1]  # Fallback to column 1

            result[area_type] = {"code": code_idx, "name": name_indices}

        return result

    def _code_name_pairs(self, df: pd.DataFrame) -> list[tuple[str, str]]:
        if df.empty or df.shape[1] < 2:
            return []

        # STEP 1: Find where data starts (after headers)
        header_idx = self._find_header_row(df)
        if header_idx is None:
            header_idx = 0  # Fallback

        # STEP 2: Determine data start position
        # If there's a sub-header after main header, data starts at header_idx + 2
        # Otherwise, data starts at header_idx + 1
        if header_idx + 1 < len(df) and self._is_sub_header_row(df.iloc[header_idx + 1]):
            data_start = header_idx + 2
        else:
            data_start = header_idx + 1

        # STEP 3: Extract data rows only
        data_df = df.iloc[data_start:, :]

        # Get column mapping once
        col_map = self._infer_column_map(df)

        pairs: list[tuple[str, str]] = []

        # Process each row
        for _, row in data_df.iterrows():
            # Get code from column 0 (always)
            code = str(row.iloc[0]).strip()
            if not code:
                continue

            # Determine area type from code length
            code_len = len(code)
            area_type: str | None = None

            if code_len == PROVINCE_CODE_LENGTH:
                area_type = "province"
            elif code_len == REGENCY_CODE_LENGTH:
                area_type = "regency"
            elif code_len == DISTRICT_CODE_LENGTH:
                area_type = "district"
            elif code_len == VILLAGE_CODE_LENGTH:
                area_type = "village"
            else:
                continue  # Invalid code format

            # Get name from appropriate columns based on area type
            # Use bfill to pick first non-empty value from matching columns
            name_col_value = col_map[area_type]["name"]
            name_indices: list[int]
            if isinstance(name_col_value, list):
                name_indices = name_col_value
            else:
                # Must be int
                name_indices = [name_col_value]

            # Filter valid indices
            valid_indices = [idx for idx in name_indices if idx < len(row)]
            if not valid_indices:
                continue

            # Get values from matching columns and pick first non-empty with alphabetic chars
            # Skip purely numeric values (counts, IDs) and only pick actual names
            name = ""
            for idx in valid_indices:
                val = str(row.iloc[idx]).strip()
                # Check if value is non-empty and contains at least some alphabetic characters
                # This filters out purely numeric values like "7" (counts) or "11.01" (codes)
                if val and any(c.isalpha() for c in val):
                    name = val
                    break

            # Fallback: if no name found in mapped columns, scan all columns after code
            # This handles cases where data is in non-standard columns
            if not name:
                for idx in range(1, len(row)):
                    val = str(row.iloc[idx]).strip()
                    if val and any(c.isalpha() for c in val):
                        name = val
                        break

            # Clean and normalize name
            if name:
                name = normalize_words(clean_name(fix_wrapped_name(name)))

            # Keep only rows that have both code and name
            if code and name:
                pairs.append((code, name))

        return pairs

    def _extract_rows(self, df: pd.DataFrame) -> dict[Area, list[list[str]]]:
        rows_by_key: dict[Area, list[list[str]]] = {
            "province": [],
            "regency": [],
            "district": [],
            "village": [],
        }
        for code, name in self._code_name_pairs(df):
            L = len(code)
            if L == PROVINCE_CODE_LENGTH:
                if code not in self._seen_provinces:
                    self._seen_provinces.add(code)
                    rows_by_key["province"].append([code, name])
            elif L == REGENCY_CODE_LENGTH:
                rows_by_key["regency"].append([code, code[:PROVINCE_CODE_LENGTH], name])
            elif L == DISTRICT_CODE_LENGTH:
                rows_by_key["district"].append([code, code[:REGENCY_CODE_LENGTH], name])
            elif L == VILLAGE_CODE_LENGTH:
                rows_by_key["village"].append([code, code[:DISTRICT_CODE_LENGTH], name])
        return rows_by_key


class IslandExtractor(TableExtractor):
    """
    Output schema (one file): code,regency_code,coordinate,is_populated,is_outermost_small,name
    """

    areas = frozenset({"island"})

    def __init__(self, destination: Path, output_name: str, config: Config) -> None:
        super().__init__(destination, output_name, config=config)

        # Get extractor config from extractors.island
        self._extractor_config = config.extractors.island
        self._fuzzy_threshold = config.fuzzy_threshold
        self._exclude_threshold = config.exclude_threshold

    # ---------- header helpers (compact) ----------
    @staticmethod
    def _norm_header_row(row: pd.Series) -> list[str]:
        # keep same normalization semantics as utils.normalize_words
        return [normalize_words(str(x)).strip().lower() for x in row.tolist()]

    @staticmethod
    def _is_island_header(headers: list[str]) -> bool:
        # Fuzzy matching rule: explicit "kode pulau" OR single "kode" while "pulau" exists
        joined = " ".join(headers)

        for h in headers:
            # Check for "kode pulau" directly
            if is_fuzzy_contained("kode pulau", h):
                return True
            # Check for "kode" and ensure "pulau" is somewhere in the headers
            if is_fuzzy_match(h, "kode"):
                if is_fuzzy_contained("pulau", joined):
                    return True

        return False

    def matches(self, df: pd.DataFrame) -> bool:
        # scan only a few top rows — tables in fixtures/banner starts here
        for i in range(min(3, len(df))):
            headers = self._norm_header_row(df.iloc[i])

            # Check for exclusion keywords first (fast rejection)
            if self._has_exclusion_keywords(headers):
                return False

            if self._is_island_header(headers):
                return True
        return False

    def _has_exclusion_keywords(self, headers: list[str]) -> bool:
        """Check if headers contain exclusion keywords."""
        # Join headers for multi-word matching
        joined = " ".join(headers)

        for keyword in self._extractor_config.exclude_keywords:
            # Exact match for short keywords like "NO"
            if keyword == "no":
                for header in headers:
                    if header == "no":
                        return True
            # Fuzzy match for other keywords
            elif is_fuzzy_contained(keyword, joined, self._exclude_threshold):
                return True

        return False

    def _infer_columns(self, headers: list[str]) -> dict[str, int | None]:
        """
        Map header -> column index in one pass, with graceful fallbacks.

        Uses configured keywords for matching with defaults as fallback.
        """

        def find_first(pred: Callable[[str], bool]) -> int | None:
            for idx, h in enumerate(headers):
                if pred(h):
                    return idx
            return None

        # Code column: must have both "kode" AND "pulau" to avoid matching name columns
        idx_code = find_first(
            lambda h: is_fuzzy_contained("kode", h, self._fuzzy_threshold)
            and is_fuzzy_contained("pulau", h, self._fuzzy_threshold)
        )
        # Fallback: just "kode"
        if idx_code is None:
            idx_code = find_first(
                lambda h: any(
                    is_fuzzy_contained(kw, h, self._fuzzy_threshold)
                    for kw in self._extractor_config.code_keywords
                )
            )

        # Name column: prefer "nama", fallback to "pulau" without "kode"
        idx_name = find_first(lambda h: is_fuzzy_contained("nama", h, self._fuzzy_threshold))
        if idx_name is None:
            # Fallback: pulau without kode
            idx_name = find_first(
                lambda h: is_fuzzy_contained("pulau", h, self._fuzzy_threshold)
                and not is_fuzzy_contained("kode", h, self._fuzzy_threshold)
            )

        # Coordinate column
        idx_coord = find_first(
            lambda h: any(
                is_fuzzy_contained(kw, h, self._fuzzy_threshold)
                for kw in self._extractor_config.coordinate_keywords
            )
        )

        # Status column
        idx_status = find_first(
            lambda h: any(
                is_fuzzy_contained(kw, h, self._fuzzy_threshold)
                if len(kw) > 3
                else is_fuzzy_match(h, kw, self._fuzzy_threshold)
                for kw in self._extractor_config.is_populated_keywords
            )
        )

        # Info column
        idx_info = find_first(
            lambda h: any(
                is_fuzzy_contained(kw, h, self._fuzzy_threshold)
                if len(kw) > 3
                else is_fuzzy_match(h, kw, self._fuzzy_threshold)
                for kw in self._extractor_config.is_outermost_small_keywords
            )
        )

        return {
            "code": idx_code,
            "name": idx_name,
            "coordinate": idx_coord,
            "status": idx_status,
            "info": idx_info,
        }

    @staticmethod
    def _parent_from_code(code: str) -> str | None:
        """Return NN.NN from NN.NN.NNNNN; return None for 'NN.00.NNNNN'."""
        prov, reg, _ = code.split(".")
        return f"{prov}.{reg}" if reg != "00" else None

    def _extract_rows(self, df: pd.DataFrame) -> dict[Area, list[list[str]]]:
        # locate header row once
        header_idx: int | None = None
        for i in range(min(4, len(df))):
            if self._is_island_header(self._norm_header_row(df.iloc[i])):
                header_idx = i
                break
        if header_idx is None:
            return {"island": []}

        headers = self._norm_header_row(df.iloc[header_idx])
        colmap = self._infer_columns(headers)

        data_df = df.iloc[header_idx + 1 :]

        rows: list[list[str]] = []

        for r in data_df.itertuples(index=False):

            def val(i: int | None) -> str:
                if i is None or i >= len(r):
                    return ""
                return str(r[i]).strip()

            code = val(colmap["code"])
            if not code or not RE_ISLAND_CODE.match(code):
                continue

            # name with "next-to-code" rescue if the name cell equals the code
            name = clean_name(fix_wrapped_name(val(colmap["name"])))
            if name == code:
                nxt = val((colmap["code"] or 0) + 1)  # safe, code idx checked above
                nxt = clean_name(fix_wrapped_name(nxt))
                if nxt and nxt != code:
                    name = nxt

            coordinate = format_coordinate(val(colmap["coordinate"]))
            status = val(colmap["status"]).upper()
            info = val(colmap["info"]).upper()

            is_populated = 1 if re.match(r"^\s*BP\b", status) else 0
            is_outermost_small = 1 if "PPKT" in info else 0
            regency_code = self._parent_from_code(code) or ""

            rows.append(
                [code, regency_code, coordinate, str(is_populated), str(is_outermost_small), name]
            )

        return {"island": rows}
