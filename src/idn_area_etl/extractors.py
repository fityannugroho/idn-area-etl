import re
from abc import ABC, abstractmethod
from pathlib import Path
from types import TracebackType
from typing import Callable, Self

import pandas as pd

from idn_area_etl.config import DEFAULT_AREA_CONFIG, DEFAULT_ISLAND_CONFIG, Area, Config
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

        # Get extractor config with defaults
        self._extractor_config = config.extractors.get("area", DEFAULT_AREA_CONFIG)
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

            # Check for 'Nama Provinsi' in second column
            has_nama_provinsi = is_fuzzy_contained(
                "nama provinsi", normalized_headers[1], self._fuzzy_threshold
            )

            if has_kode and has_nama_provinsi:
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

    def _infer_name_columns(self, df: pd.DataFrame) -> list[int]:
        """Identify name columns using fuzzy matching on headers."""
        if df.empty or df.shape[0] < 1:
            return [1]  # Fallback to column 1

        # Check rows 0-2 for headers (like matches() method)
        matched_cols: set[int] = set()
        for row_idx in range(min(3, len(df))):
            headers = [normalize_words(str(col)).lower() for col in df.iloc[row_idx]]

            # Find columns matching name_keywords (exclude col 0 which is code)
            for idx in range(1, len(headers)):
                for keyword in self._extractor_config.name_keywords:
                    if is_fuzzy_contained(keyword, headers[idx], self._fuzzy_threshold):
                        matched_cols.add(idx)
                        break  # Move to next column after first match

        # Return matched indices sorted, fallback to [1] if none
        return sorted(list(matched_cols)) if matched_cols else [1]

    def _code_name_pairs(self, df: pd.DataFrame) -> list[tuple[str, str]]:
        if df.empty or df.shape[1] < 2:
            return []

        # Skip header rows; keep only data rows
        data_df = df.iloc[2:, :]

        # Codes as string, strip spaces
        codes = data_df.iloc[:, 0].astype(str).str.strip()

        # Dynamically infer name columns based on headers
        name_cols = self._infer_name_columns(df)

        # Validate column indices exist in DataFrame
        valid_name_cols = [col for col in name_cols if col < data_df.shape[1]]
        if not valid_name_cols:
            valid_name_cols = [1]  # Fallback to column 1

        # Pick the first non-empty candidate per row, then clean/normalize
        names = (
            data_df.iloc[:, valid_name_cols]
            .astype(str)
            .map(str.strip)  # element-wise strip
            .replace("", pd.NA)
            .bfill(axis=1)
            .iloc[:, 0]
            .fillna("")
            .map(lambda s: normalize_words(clean_name(fix_wrapped_name(s))) if s else "")
        )
        # Keep only rows that have both code and name
        mask = codes.ne("") & names.ne("")
        return list(zip(codes[mask].tolist(), names[mask].tolist()))

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

        # Get extractor config with defaults
        self._extractor_config = config.extractors.get("island", DEFAULT_ISLAND_CONFIG)
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
                for kw in self._extractor_config.status_keywords
            )
        )

        # Info column
        idx_info = find_first(
            lambda h: any(
                is_fuzzy_contained(kw, h, self._fuzzy_threshold)
                if len(kw) > 3
                else is_fuzzy_match(h, kw, self._fuzzy_threshold)
                for kw in self._extractor_config.info_keywords
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
