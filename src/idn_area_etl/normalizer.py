"""
CSV normalization for extracted Indonesian area data.

Uses ground truth data and fuzzy matching to suggest corrections
for dirty or inconsistent data extracted from PDFs.
"""

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Literal

from idn_area_etl.config import Area
from idn_area_etl.ground_truth import (
    PARENT_FIELD_MAP,
    AreaIndex,
    AreaRecord,
    GroundTruthIndex,
    IslandRecord,
)
from idn_area_etl.utils import MatchCandidate

NormalizationStatus = Literal["valid", "corrected", "ambiguous", "not_found"]


def _empty_suggestion_list() -> list["NormalizationSuggestion"]:
    return []


@dataclass
class NormalizationSuggestion:
    """A suggested correction for a field value."""

    original: str
    suggested: str
    confidence: float
    reason: str


@dataclass
class RowNormalization:
    """
    Normalization result for a single row.

    Contains the original row, suggested corrections, and overall status.
    """

    row_number: int
    original: dict[str, str]
    corrected: dict[str, str]
    status: NormalizationStatus
    suggestions: list[NormalizationSuggestion] = field(default_factory=_empty_suggestion_list)

    def has_changes(self) -> bool:
        """Check if any corrections were made."""
        return self.original != self.corrected

    def add_suggestion(self, suggestion: NormalizationSuggestion) -> None:
        """Add a suggestion and update corrected dict."""
        self.suggestions.append(suggestion)


def _empty_row_normalization_list() -> list["RowNormalization"]:
    return []


@dataclass
class NormalizationReport:
    """Aggregated normalization results for a CSV file."""

    area: Area
    total_rows: int = 0
    valid_rows: int = 0
    corrected_rows: int = 0
    ambiguous_rows: int = 0
    not_found_rows: int = 0
    normalizations: list[RowNormalization] = field(default_factory=_empty_row_normalization_list)

    def add(self, row_norm: RowNormalization) -> None:
        """Add a row normalization result."""
        self.normalizations.append(row_norm)
        self.total_rows += 1
        if row_norm.status == "valid":
            self.valid_rows += 1
        elif row_norm.status == "corrected":
            self.corrected_rows += 1
        elif row_norm.status == "ambiguous":
            self.ambiguous_rows += 1
        elif row_norm.status == "not_found":
            self.not_found_rows += 1

    def summary(self) -> str:
        """Return a human-readable summary of the normalization results."""
        return (
            f"Area: {self.area}\n"
            f"Total rows: {self.total_rows}\n"
            f"Valid rows: {self.valid_rows}\n"
            f"Corrected rows: {self.corrected_rows}\n"
            f"Ambiguous rows: {self.ambiguous_rows}\n"
            f"Not found rows: {self.not_found_rows}"
        )

    def write_corrected_csv(self, output_path: Path, headers: list[str]) -> None:
        """Write corrected rows to a CSV file."""
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row_norm in self.normalizations:
                writer.writerow(row_norm.corrected)

    def write_report_csv(self, output_path: Path) -> None:
        """Write normalization report to a CSV file."""
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["row_number", "status", "column", "original", "suggested", "confidence", "reason"]
            )
            for row_norm in self.normalizations:
                if row_norm.suggestions:
                    for sug in row_norm.suggestions:
                        writer.writerow(
                            [
                                row_norm.row_number,
                                row_norm.status,
                                self._find_column(row_norm.original, sug.original),
                                sug.original,
                                sug.suggested,
                                f"{sug.confidence:.1f}",
                                sug.reason,
                            ]
                        )
                elif row_norm.status != "valid":
                    # Row with issues but no specific suggestions
                    writer.writerow(
                        [
                            row_norm.row_number,
                            row_norm.status,
                            "",
                            "",
                            "",
                            "",
                            "No matching ground truth found",
                        ]
                    )

    def _find_column(self, row: dict[str, str], value: str) -> str:
        """Find which column contains the given value."""
        for col, val in row.items():
            if val == value:
                return col
        return "unknown"


class Normalizer:
    """
    Normalizes extracted area data against ground truth.

    Uses fuzzy matching to find and suggest corrections for
    names that don't exactly match ground truth data.
    """

    def __init__(
        self,
        ground_truth: GroundTruthIndex,
        *,
        confidence_threshold: float = 80.0,
        ambiguity_threshold: float = 5.0,
    ) -> None:
        """
        Initialize the normalizer.

        Args:
            ground_truth: The ground truth index to match against
            confidence_threshold: Minimum score (0-100) to consider a match valid
            ambiguity_threshold: Score gap required between top matches to avoid ambiguity
        """
        self.ground_truth = ground_truth
        self.confidence_threshold = confidence_threshold
        self.ambiguity_threshold = ambiguity_threshold

        # Map area types to their index and normalization method
        self._area_indices: dict[Area, AreaIndex] = {
            "province": ground_truth.provinces,
            "regency": ground_truth.regencies,
            "district": ground_truth.districts,
            "village": ground_truth.villages,
        }

        # Dispatcher for normalize_row
        self._normalizers: dict[Area, Callable[[dict[str, str], int], RowNormalization]] = {
            "province": self._normalize_province,
            "regency": self._normalize_area_with_parent,
            "district": self._normalize_area_with_parent,
            "village": self._normalize_area_with_parent,
            "island": self._normalize_island,
        }

    def normalize_row(self, row: dict[str, str], row_num: int, area: Area) -> RowNormalization:
        """Normalize a single row based on area type."""
        if area in ("regency", "district", "village"):
            return self._normalize_area_with_parent(row, row_num, area)
        normalizer = self._normalizers.get(area)
        if normalizer is None:
            raise ValueError(f"Unknown area type: {area}")
        return normalizer(row, row_num)

    def _normalize_province(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a province row (no parent code)."""
        code = row.get("code", "")
        name = row.get("name", "")
        corrected = dict(row)

        record = self.ground_truth.provinces.get_by_code(code)
        if record:
            if record.name == name:
                return RowNormalization(
                    row_number=row_num,
                    original=row,
                    corrected=corrected,
                    status="valid",
                )
            # Name mismatch - suggest correction from ground truth
            suggestion = NormalizationSuggestion(
                original=name,
                suggested=record.name,
                confidence=100.0,
                reason=f"Name corrected to match code {code} in ground truth",
            )
            corrected["name"] = record.name
            return RowNormalization(
                row_number=row_num,
                original=row,
                corrected=corrected,
                status="corrected",
                suggestions=[suggestion],
            )

        # Code not found - try fuzzy match on name
        return self._normalize_by_name(row, row_num, name, "province", corrected, parent_code=None)

    def _normalize_area_with_parent(
        self, row: dict[str, str], row_num: int, area: Area | None = None
    ) -> RowNormalization:
        """
        Normalize an area row that has a parent code field.

        Works for regency, district, and village areas.
        """
        # Infer area from row structure if not provided
        if area is None:
            if "province_code" in row:
                area = "regency"
            elif "regency_code" in row:
                area = "district"
            elif "district_code" in row:
                area = "village"
            else:
                raise ValueError("Cannot infer area type from row")

        parent_field = PARENT_FIELD_MAP.get(area, "")
        index = self._area_indices.get(area)
        if index is None:
            raise ValueError(f"No index for area type: {area}")

        code = row.get("code", "")
        parent_code = row.get(parent_field, "") if parent_field else ""
        name = row.get("name", "")
        corrected = dict(row)

        record = index.get_by_code(code)
        if record:
            return self._build_correction_from_record(
                row, row_num, corrected, record, name, parent_code, parent_field, code
            )

        # Code not found - try fuzzy match on name within parent context
        return self._normalize_by_name(row, row_num, name, area, corrected, parent_code=parent_code)

    def _build_correction_from_record(
        self,
        row: dict[str, str],
        row_num: int,
        corrected: dict[str, str],
        record: AreaRecord,
        name: str,
        parent_code: str,
        parent_field: str,
        code: str,
    ) -> RowNormalization:
        """Build correction suggestions from a matched record."""
        suggestions: list[NormalizationSuggestion] = []

        if record.name != name:
            suggestions.append(
                NormalizationSuggestion(
                    original=name,
                    suggested=record.name,
                    confidence=100.0,
                    reason=f"Name corrected to match code {code} in ground truth",
                )
            )
            corrected["name"] = record.name

        if parent_field and record.parent_code != parent_code:
            suggestions.append(
                NormalizationSuggestion(
                    original=parent_code,
                    suggested=record.parent_code,
                    confidence=100.0,
                    reason=f"Parent code corrected to match code {code} in ground truth",
                )
            )
            corrected[parent_field] = record.parent_code

        if suggestions:
            return RowNormalization(
                row_number=row_num,
                original=row,
                corrected=corrected,
                status="corrected",
                suggestions=suggestions,
            )
        return RowNormalization(
            row_number=row_num,
            original=row,
            corrected=corrected,
            status="valid",
        )

    def _normalize_island(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize an island row."""
        code = row.get("code", "")
        regency_code = row.get("regency_code", "")
        name = row.get("name", "")
        corrected = dict(row)

        record = self.ground_truth.islands.get_by_code(code)
        if record:
            return self._build_island_correction_from_record(
                row, row_num, corrected, record, name, regency_code, code
            )

        # Code not found - try fuzzy match on name within regency context
        return self._normalize_island_by_name(row, row_num, name, corrected, regency_code)

    def _build_island_correction_from_record(
        self,
        row: dict[str, str],
        row_num: int,
        corrected: dict[str, str],
        record: IslandRecord,
        name: str,
        regency_code: str,
        code: str,
    ) -> RowNormalization:
        """Build correction suggestions for an island from a matched record."""
        suggestions: list[NormalizationSuggestion] = []

        if record.name != name:
            suggestions.append(
                NormalizationSuggestion(
                    original=name,
                    suggested=record.name,
                    confidence=100.0,
                    reason=f"Name corrected to match code {code} in ground truth",
                )
            )
            corrected["name"] = record.name

        if record.regency_code != regency_code:
            suggestions.append(
                NormalizationSuggestion(
                    original=regency_code,
                    suggested=record.regency_code,
                    confidence=100.0,
                    reason=f"Regency code corrected to match code {code} in ground truth",
                )
            )
            corrected["regency_code"] = record.regency_code

        # Also normalize coordinate from ground truth if different
        if record.coordinate and row.get("coordinate", "") != record.coordinate:
            suggestions.append(
                NormalizationSuggestion(
                    original=row.get("coordinate", ""),
                    suggested=record.coordinate,
                    confidence=100.0,
                    reason=f"Coordinate corrected to match code {code} in ground truth",
                )
            )
            corrected["coordinate"] = record.coordinate

        if suggestions:
            return RowNormalization(
                row_number=row_num,
                original=row,
                corrected=corrected,
                status="corrected",
                suggestions=suggestions,
            )
        return RowNormalization(
            row_number=row_num,
            original=row,
            corrected=corrected,
            status="valid",
        )

    def _normalize_by_name(
        self,
        row: dict[str, str],
        row_num: int,
        name: str,
        area: Area,
        corrected: dict[str, str],
        parent_code: str | None,
    ) -> RowNormalization:
        """
        Normalize a row by fuzzy matching the name field.

        Returns appropriate status based on match quality.
        """
        matches = self.ground_truth.search_name_in_context(
            name, area, parent_code, n=3, threshold=self.confidence_threshold
        )

        if not matches:
            return RowNormalization(
                row_number=row_num,
                original=row,
                corrected=corrected,
                status="not_found",
            )

        return self._evaluate_matches(row, row_num, name, corrected, matches, area)

    def _normalize_island_by_name(
        self,
        row: dict[str, str],
        row_num: int,
        name: str,
        corrected: dict[str, str],
        regency_code: str,
    ) -> RowNormalization:
        """Normalize an island row by fuzzy matching the name field."""
        matches = self.ground_truth.search_name_in_context(
            name, "island", regency_code, n=3, threshold=self.confidence_threshold
        )

        if not matches:
            return RowNormalization(
                row_number=row_num,
                original=row,
                corrected=corrected,
                status="not_found",
            )

        # Check for ambiguity
        if ambiguous := self._check_ambiguity(row, row_num, name, corrected, matches):
            return ambiguous

        # Use top match
        top_match = matches[0]
        record = self.ground_truth.islands.get_by_code(top_match.key or "")
        if record:
            suggestions = [
                NormalizationSuggestion(
                    original=name,
                    suggested=record.name,
                    confidence=top_match.score,
                    reason=f"Fuzzy matched to island code {record.code}",
                )
            ]
            corrected["name"] = record.name
            corrected["code"] = record.code
            if record.regency_code:
                corrected["regency_code"] = record.regency_code
            if record.coordinate:
                corrected["coordinate"] = record.coordinate
            if record.is_populated:
                corrected["is_populated"] = record.is_populated
            if record.is_outermost_small:
                corrected["is_outermost_small"] = record.is_outermost_small

            return RowNormalization(
                row_number=row_num,
                original=row,
                corrected=corrected,
                status="corrected",
                suggestions=suggestions,
            )

        return RowNormalization(
            row_number=row_num,
            original=row,
            corrected=corrected,
            status="not_found",
        )

    def _check_ambiguity(
        self,
        row: dict[str, str],
        row_num: int,
        name: str,
        corrected: dict[str, str],
        matches: list[MatchCandidate],
    ) -> RowNormalization | None:
        """Check if matches are ambiguous and return ambiguous result if so."""
        if len(matches) > 1:
            top_match = matches[0]
            score_gap = top_match.score - matches[1].score
            if score_gap < self.ambiguity_threshold:
                return RowNormalization(
                    row_number=row_num,
                    original=row,
                    corrected=corrected,
                    status="ambiguous",
                    suggestions=[
                        NormalizationSuggestion(
                            original=name,
                            suggested=m.value,
                            confidence=m.score,
                            reason=f"Fuzzy match candidate (code: {m.key})",
                        )
                        for m in matches
                    ],
                )
        return None

    def _evaluate_matches(
        self,
        row: dict[str, str],
        row_num: int,
        name: str,
        corrected: dict[str, str],
        matches: list[MatchCandidate],
        area: Area,
    ) -> RowNormalization:
        """Evaluate fuzzy matches and return appropriate normalization result."""
        # Check for ambiguity
        if ambiguous := self._check_ambiguity(row, row_num, name, corrected, matches):
            return ambiguous

        # Use top match - get full record
        top_match = matches[0]
        record = self._get_record_by_code(area, top_match.key or "")
        if record:
            parent_field = PARENT_FIELD_MAP.get(area, "")
            suggestions = [
                NormalizationSuggestion(
                    original=name,
                    suggested=record.name,
                    confidence=top_match.score,
                    reason=f"Fuzzy matched to {area} code {record.code}",
                )
            ]
            corrected["name"] = record.name
            corrected["code"] = record.code
            if record.parent_code and parent_field:
                corrected[parent_field] = record.parent_code

            return RowNormalization(
                row_number=row_num,
                original=row,
                corrected=corrected,
                status="corrected",
                suggestions=suggestions,
            )

        return RowNormalization(
            row_number=row_num,
            original=row,
            corrected=corrected,
            status="not_found",
        )

    def _get_record_by_code(self, area: Area, code: str) -> AreaRecord | None:
        """Get an area record by code."""
        index = self._area_indices.get(area)
        return index.get_by_code(code) if index else None

    # Keep old method names as aliases for backwards compatibility
    def normalize_province(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a province row."""
        return self._normalize_province(row, row_num)

    def normalize_regency(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a regency row."""
        return self._normalize_area_with_parent(row, row_num, "regency")

    def normalize_district(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a district row."""
        return self._normalize_area_with_parent(row, row_num, "district")

    def normalize_village(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a village row."""
        return self._normalize_area_with_parent(row, row_num, "village")

    def normalize_island(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize an island row."""
        return self._normalize_island(row, row_num)


def normalize_csv(
    file_path: Path,
    area: Area,
    ground_truth: GroundTruthIndex,
    *,
    confidence_threshold: float = 80.0,
    ambiguity_threshold: float = 5.0,
) -> NormalizationReport:
    """
    Normalize a CSV file against ground truth data.

    Args:
        file_path: Path to the CSV file to normalize
        area: Area type for normalization
        ground_truth: Ground truth index to match against
        confidence_threshold: Minimum score for valid matches
        ambiguity_threshold: Score gap required between top matches

    Returns:
        NormalizationReport with all normalization results
    """
    normalizer = Normalizer(
        ground_truth,
        confidence_threshold=confidence_threshold,
        ambiguity_threshold=ambiguity_threshold,
    )
    report = NormalizationReport(area=area)

    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
            row_norm = normalizer.normalize_row(row, row_num, area)
            report.add(row_norm)

    return report
