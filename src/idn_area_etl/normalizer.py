"""
CSV normalization for extracted Indonesian area data.

Uses ground truth data and fuzzy matching to suggest corrections
for dirty or inconsistent data extracted from PDFs.
"""

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from idn_area_etl.config import Area
from idn_area_etl.ground_truth import AreaRecord, GroundTruthIndex
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

    def normalize_province(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a province row."""
        code = row.get("code", "")
        name = row.get("name", "")
        corrected = dict(row)

        # Check exact match by code
        record = self.ground_truth.provinces.get_by_code(code)
        if record:
            if record.name == name:
                return RowNormalization(
                    row_number=row_num,
                    original=row,
                    corrected=corrected,
                    status="valid",
                )
            else:
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

    def normalize_regency(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a regency row."""
        code = row.get("code", "")
        province_code = row.get("province_code", "")
        name = row.get("name", "")
        corrected = dict(row)

        # Check exact match by code
        record = self.ground_truth.regencies.get_by_code(code)
        if record:
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

            if record.parent_code != province_code:
                suggestions.append(
                    NormalizationSuggestion(
                        original=province_code,
                        suggested=record.parent_code,
                        confidence=100.0,
                        reason=f"Province code corrected to match code {code} in ground truth",
                    )
                )
                corrected["province_code"] = record.parent_code

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

        # Code not found - try fuzzy match on name within province context
        return self._normalize_by_name(
            row, row_num, name, "regency", corrected, parent_code=province_code
        )

    def normalize_district(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a district row."""
        code = row.get("code", "")
        regency_code = row.get("regency_code", "")
        name = row.get("name", "")
        corrected = dict(row)

        # Check exact match by code
        record = self.ground_truth.districts.get_by_code(code)
        if record:
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

            if record.parent_code != regency_code:
                suggestions.append(
                    NormalizationSuggestion(
                        original=regency_code,
                        suggested=record.parent_code,
                        confidence=100.0,
                        reason=f"Regency code corrected to match code {code} in ground truth",
                    )
                )
                corrected["regency_code"] = record.parent_code

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

        # Code not found - try fuzzy match on name within regency context
        return self._normalize_by_name(
            row, row_num, name, "district", corrected, parent_code=regency_code
        )

    def normalize_village(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize a village row."""
        code = row.get("code", "")
        district_code = row.get("district_code", "")
        name = row.get("name", "")
        corrected = dict(row)

        # Check exact match by code
        record = self.ground_truth.villages.get_by_code(code)
        if record:
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

            if record.parent_code != district_code:
                suggestions.append(
                    NormalizationSuggestion(
                        original=district_code,
                        suggested=record.parent_code,
                        confidence=100.0,
                        reason=f"District code corrected to match code {code} in ground truth",
                    )
                )
                corrected["district_code"] = record.parent_code

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

        # Code not found - try fuzzy match on name within district context
        return self._normalize_by_name(
            row, row_num, name, "village", corrected, parent_code=district_code
        )

    def normalize_island(self, row: dict[str, str], row_num: int) -> RowNormalization:
        """Normalize an island row."""
        code = row.get("code", "")
        regency_code = row.get("regency_code", "")
        name = row.get("name", "")
        corrected = dict(row)

        # Check exact match by code
        record = self.ground_truth.islands.get_by_code(code)
        if record:
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

            # Also normalize coordinate, is_populated, is_outermost_small from ground truth
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

        # Code not found - try fuzzy match on name within regency context
        return self._normalize_island_by_name(row, row_num, name, corrected, regency_code)

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

        top_match = matches[0]

        # Check for ambiguity
        if len(matches) > 1:
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

        # Use top match
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
        top_match = matches[0]

        # Check for ambiguity
        if len(matches) > 1:
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

        # Use top match - get full record
        record = self._get_record_by_code(area, top_match.key or "")
        if record:
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
            if record.parent_code:
                parent_field = self._get_parent_field(area)
                if parent_field:
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
        if area == "province":
            return self.ground_truth.provinces.get_by_code(code)
        elif area == "regency":
            return self.ground_truth.regencies.get_by_code(code)
        elif area == "district":
            return self.ground_truth.districts.get_by_code(code)
        elif area == "village":
            return self.ground_truth.villages.get_by_code(code)
        return None

    def _get_parent_field(self, area: Area) -> str:
        """Get the parent code field name for an area type."""
        fields: dict[Area, str] = {
            "province": "",
            "regency": "province_code",
            "district": "regency_code",
            "village": "district_code",
            "island": "regency_code",
        }
        return fields.get(area, "")

    def normalize_row(self, row: dict[str, str], row_num: int, area: Area) -> RowNormalization:
        """Normalize a single row based on area type."""
        if area == "province":
            return self.normalize_province(row, row_num)
        elif area == "regency":
            return self.normalize_regency(row, row_num)
        elif area == "district":
            return self.normalize_district(row, row_num)
        elif area == "village":
            return self.normalize_village(row, row_num)
        elif area == "island":
            return self.normalize_island(row, row_num)
        else:
            raise ValueError(f"Unknown area type: {area}")


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
