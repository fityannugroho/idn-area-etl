import csv
import re
from abc import ABC, abstractmethod
from io import TextIOWrapper
from pathlib import Path
from types import TracebackType
from typing import Any, Callable

import pandas as pd

from idn_area_etl.utils import (
    PROVINCE_CODE_LENGTH,
    REGENCY_CODE_LENGTH,
    DISTRICT_CODE_LENGTH,
    VILLAGE_CODE_LENGTH,
    RE_ISLAND_CODE,
    clean_name,
    fix_wrapped_name,
    format_coordinate,
    normalize_words,
)


class TableExtractor(ABC):
    """
    Base extractor that:
     - decides matching tables (matches)
     - extracts ready rows (extract_rows)
     - writes rows to its own CSV target(s) with buffering
    """

    def __init__(self, destination: Path, output_name: str) -> None:
        self.destination = destination
        self.output_name = output_name
        self.file_handles: dict[str, TextIOWrapper] = {}
        self.writers: dict[str, Any] = {}
        self.buffers: dict[str, list[list[str]]] = {}
        self.batch_sizes: dict[str, int] = {}
        self.headers: dict[str, list[str]] = {}

    def __enter__(self) -> "TableExtractor":
        """Open all target output files and return self."""
        self.open_outputs()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Ensure outputs are flushed and closed even if an exception occurs.

        Returning False (or None) lets any exception propagate (desired).
        """
        self.close_outputs()
        return False

    @abstractmethod
    def targets(self) -> dict[str, tuple[str, list[str], int]]:
        """
        Return targets mapping:
          key -> (filename_suffix, headers, batch_size)
        e.g. for island: {"island": ("island", ["code","parent",...], 1000)}
        """
        ...

    def open_outputs(self) -> None:
        for key, (suffix, headers, batch) in self.targets().items():
            path = self.destination / f"{self.output_name}.{suffix}.csv"
            fh = open(path, mode="w", newline="", encoding="utf-8", buffering=1048576)
            writer = csv.writer(fh)
            writer.writerow(headers)
            self.file_handles[key] = fh
            self.writers[key] = writer
            self.buffers[key] = []
            self.headers[key] = headers
            self.batch_sizes[key] = batch

    def close_outputs(self) -> None:
        for key in list(self.file_handles.keys()):
            self.flush(key)
            try:
                self.file_handles[key].close()
            except Exception:
                pass
        self.file_handles.clear()
        self.writers.clear()
        self.buffers.clear()

    def flush(self, key: str) -> None:
        buf = self.buffers.get(key, [])
        if buf:
            self.writers[key].writerows(buf)
            self.file_handles[key].flush()
            self.buffers[key] = []

    def write_rows(self, key: str, rows: list[list[str]]) -> None:
        if not rows:
            return
        self.buffers[key].extend(rows)
        if len(self.buffers[key]) >= self.batch_sizes[key]:
            self.flush(key)

    @abstractmethod
    def matches(self, df: pd.DataFrame) -> bool: ...

    @abstractmethod
    def extract_rows(self, df: pd.DataFrame) -> dict[str, list[list[str]]]:
        """
        Returns a dict keyed by target key (same as in self.targets())
        with lists of rows already matching final CSV schema.
        """
        ...

    def extract_and_write(self, df: pd.DataFrame) -> int:
        data = self.extract_rows(df)
        total = 0
        for key, rows in data.items():
            self.write_rows(key, rows)
            total += len(rows)
        return total


class AreaExtractor(TableExtractor):
    """
    Handle four outputs at once: province/regency/district/village.
    """

    def __init__(self, destination: Path, output_name: str) -> None:
        super().__init__(destination, output_name)
        self._seen_provinces: set[str] = set()

    def targets(self) -> dict[str, tuple[str, list[str], int]]:
        return {
            "province": ("province", ["code", "name"], 500),
            "regency": ("regency", ["code", "province_code", "name"], 500),
            "district": ("district", ["code", "regency_code", "name"], 1000),
            "village": ("village", ["code", "district_code", "name"], 2000),
        }

    def matches(self, df: pd.DataFrame) -> bool:
        if df.empty or df.shape[0] < 1:
            return False
        normalized_headers = [normalize_words(str(col)).lower() for col in df.iloc[0]]
        return (
            len(normalized_headers) >= 2
            and normalized_headers[0] == "kode"
            and "nama provinsi" in normalized_headers[1]
        )

    def _code_name_pairs(self, df: pd.DataFrame) -> list[tuple[str, str]]:
        if df.empty or df.shape[1] < 2:
            return []

        # Skip header rows; keep only data rows
        data_df = df.iloc[2:, :]

        # Codes as string, strip spaces
        codes = data_df.iloc[:, 0].astype(str).str.strip()

        # Decide name columns based on table variant:
        # 6-column tables -> use columns [1, 3]
        # Wider tables (>=7 columns) -> use [1, 4, 5, 6]
        if data_df.shape[1] == 6:
            name_cols = [1, 3]
        else:
            name_cols = [1, 4, 5, 6]

        # Pick the first non-empty candidate per row, then clean/normalize
        names = (
            data_df.iloc[:, name_cols]
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

    def extract_rows(self, df: pd.DataFrame) -> dict[str, list[list[str]]]:
        rows_by_key: dict[str, list[list[str]]] = {
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

    def targets(self) -> dict[str, tuple[str, list[str], int]]:
        return {
            "island": (
                "island",
                [
                    "code",
                    "regency_code",
                    "coordinate",
                    "is_populated",
                    "is_outermost_small",
                    "name",
                ],
                1000,
            )
        }

    # ---------- header helpers (compact) ----------
    @staticmethod
    def _norm_header_row(row: pd.Series) -> list[str]:
        # keep same normalization semantics as utils.normalize_words
        return [normalize_words(str(x)).strip().lower() for x in row.tolist()]

    @staticmethod
    def _is_island_header(headers: list[str]) -> bool:
        # same matching rule as before: explicit "kode pulau" OR single "kode" while "pulau" exists
        joined = " ".join(headers)
        return any(("kode pulau" in h) or (h == "kode" and "pulau" in joined) for h in headers)

    def matches(self, df: pd.DataFrame) -> bool:
        # scan only a few top rows — tables in fixtures/banner starts here
        for i in range(min(3, len(df))):
            if self._is_island_header(self._norm_header_row(df.iloc[i])):
                return True
        return False

    @staticmethod
    def _infer_columns(headers: list[str]) -> dict[str, int | None]:
        """
        Map header -> column index in one pass, with graceful fallbacks.

        Rules:
          - code:  "kode pulau" > "kode"
          - name:  contains "nama" OR contains "pulau" without "kode"
               else fallback to (idx_code + 1) if there is a column to the right.
          - coordinate: "koordinat" | "kordinat"
          - status:  "bp/tbp" | "status" | "bp" | "tbp" | "keterangan"
          - info:    "keterangan" | "ket"
        """

        def find_first(pred: Callable[[str], bool]) -> int | None:
            for idx, h in enumerate(headers):
                if pred(h):
                    return idx
            return None

        idx_code = find_first(lambda h: "kode" in h and "pulau" in h)

        idx_name = find_first(lambda h: "nama" in h)

        idx_coord = find_first(lambda h: ("koordinat" in h) or ("kordinat" in h))

        idx_status = find_first(
            lambda h: ("bp/tbp" in h) or (h in ("bp", "tbp", "status")) or ("keterangan" in h)
        )
        idx_info = find_first(lambda h: ("keterangan" in h) or (h == "ket"))

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

    def extract_rows(self, df: pd.DataFrame) -> dict[str, list[list[str]]]:
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
