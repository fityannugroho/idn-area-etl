"""
CSV validation for extracted Indonesian area data.

Provides validators for each area type (province, regency, district, village, island)
to check data integrity and report errors.
"""

import csv
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from idn_area_etl.config import Area
from idn_area_etl.utils import (
    DISTRICT_CODE_LENGTH,
    PROVINCE_CODE_LENGTH,
    RE_COORDINATE,
    RE_DISTRICT_CODE,
    RE_ISLAND_CODE,
    RE_PROVINCE_CODE,
    RE_REGENCY_CODE,
    RE_VILLAGE_CODE,
    REGENCY_CODE_LENGTH,
)


@dataclass
class ValidationError:
    """Represents a single validation error for a CSV row."""

    row_number: int
    column: str
    value: str
    error_type: str
    message: str


def _empty_error_list() -> list["ValidationError"]:
    return []


@dataclass
class ValidationReport:
    """Aggregated validation results for a CSV file."""

    area: Area
    total_rows: int
    valid_rows: int = 0
    invalid_rows: int = 0
    errors: list["ValidationError"] = field(default_factory=_empty_error_list)

    def add_error(self, error: ValidationError) -> None:
        self.errors.append(error)

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def to_csv(self, path: Path) -> None:
        """Write validation errors to a CSV report file."""
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["row_number", "column", "value", "error_type", "message"])
            for err in self.errors:
                writer.writerow(
                    [err.row_number, err.column, err.value, err.error_type, err.message]
                )

    def summary(self) -> str:
        """Return a human-readable summary of the validation results."""
        return (
            f"Area: {self.area}\n"
            f"Total rows: {self.total_rows}\n"
            f"Valid rows: {self.valid_rows}\n"
            f"Invalid rows: {self.invalid_rows}\n"
            f"Total errors: {len(self.errors)}"
        )


class RowValidator(ABC):
    """Base class for row validators using Template Method pattern."""

    area: Area
    expected_headers: tuple[str, ...]

    @abstractmethod
    def validate_row(self, row: dict[str, str], row_num: int) -> list[ValidationError]:
        """Validate a single row and return list of errors (empty if valid)."""
        ...

    def validate_headers(self, headers: list[str]) -> ValidationError | None:
        """Validate that CSV headers match expected headers for this area."""
        expected = list(self.expected_headers)
        if headers != expected:
            return ValidationError(
                row_number=0,
                column="headers",
                value=",".join(headers),
                error_type="invalid_headers",
                message=f"Expected headers {expected}, got {headers}",
            )
        return None

    def _validate_code_length(
        self, code: str, expected_length: int, row_num: int, column: str = "code"
    ) -> ValidationError | None:
        """Validate that a code has the expected length."""
        if len(code) != expected_length:
            return ValidationError(
                row_number=row_num,
                column=column,
                value=code,
                error_type="invalid_code_length",
                message=f"Code must be exactly {expected_length} digits, got {len(code)}",
            )
        return None

    def _validate_code_numeric(
        self, code: str, row_num: int, column: str = "code"
    ) -> ValidationError | None:
        """Validate that a code contains only digits."""
        if not code.isdigit():
            return ValidationError(
                row_number=row_num,
                column=column,
                value=code,
                error_type="invalid_code_format",
                message=f"Code must be numeric, got '{code}'",
            )
        return None

    def _validate_code_pattern(
        self,
        code: str,
        pattern: re.Pattern[str],
        expected_format: str,
        row_num: int,
        column: str = "code",
    ) -> ValidationError | None:
        """Validate that a code matches the expected regex pattern."""
        if not pattern.match(code):
            return ValidationError(
                row_number=row_num,
                column=column,
                value=code,
                error_type="invalid_code_format",
                message=f"Code must match pattern {expected_format}, got '{code}'",
            )
        return None

    def _validate_not_empty(self, value: str, row_num: int, column: str) -> ValidationError | None:
        """Validate that a value is not empty."""
        if not value.strip():
            return ValidationError(
                row_number=row_num,
                column=column,
                value=value,
                error_type="empty_value",
                message=f"{column.capitalize()} cannot be empty",
            )
        return None

    def _validate_parent_code_prefix(
        self,
        code: str,
        parent_code: str,
        parent_length: int,
        row_num: int,
        parent_column: str,
    ) -> ValidationError | None:
        """Validate that parent code matches the prefix of child code."""
        expected_prefix = code[:parent_length]
        if parent_code != expected_prefix:
            return ValidationError(
                row_number=row_num,
                column=parent_column,
                value=parent_code,
                error_type="invalid_parent_code",
                message=(
                    f"Parent code '{parent_code}' doesn't match code prefix '{expected_prefix}'"
                ),
            )
        return None

    def _validate_boolean(self, value: str, row_num: int, column: str) -> ValidationError | None:
        """Validate that a value is a boolean string ('0' or '1')."""
        if value not in ("0", "1"):
            return ValidationError(
                row_number=row_num,
                column=column,
                value=value,
                error_type="invalid_boolean",
                message=f"{column} must be '0' or '1', got '{value}'",
            )
        return None


class ProvinceValidator(RowValidator):
    """Validator for province data."""

    area: Area = "province"
    expected_headers = ("code", "name")

    def validate_row(self, row: dict[str, str], row_num: int) -> list[ValidationError]:
        errors: list[ValidationError] = []
        code = row.get("code", "")
        name = row.get("name", "")

        # Validate code
        if err := self._validate_not_empty(code, row_num, "code"):
            errors.append(err)
        else:
            if err := self._validate_code_length(code, PROVINCE_CODE_LENGTH, row_num):
                errors.append(err)
            if err := self._validate_code_numeric(code, row_num):
                errors.append(err)

        # Validate name
        if err := self._validate_not_empty(name, row_num, "name"):
            errors.append(err)

        return errors


class RegencyValidator(RowValidator):
    """Validator for regency data."""

    area: Area = "regency"
    expected_headers = ("code", "province_code", "name")

    def validate_row(self, row: dict[str, str], row_num: int) -> list[ValidationError]:
        errors: list[ValidationError] = []
        code = row.get("code", "")
        province_code = row.get("province_code", "")
        name = row.get("name", "")

        # Validate code (format: NN.NN, e.g., "11.01")
        if err := self._validate_not_empty(code, row_num, "code"):
            errors.append(err)
        else:
            if err := self._validate_code_pattern(code, RE_REGENCY_CODE, "NN.NN", row_num):
                errors.append(err)

        # Validate province_code (format: NN, e.g., "11")
        if err := self._validate_not_empty(province_code, row_num, "province_code"):
            errors.append(err)
        else:
            if err := self._validate_code_pattern(
                province_code, RE_PROVINCE_CODE, "NN", row_num, "province_code"
            ):
                errors.append(err)
            # Validate parent code matches prefix
            elif code and RE_REGENCY_CODE.match(code):
                if err := self._validate_parent_code_prefix(
                    code, province_code, PROVINCE_CODE_LENGTH, row_num, "province_code"
                ):
                    errors.append(err)

        # Validate name
        if err := self._validate_not_empty(name, row_num, "name"):
            errors.append(err)

        return errors


class DistrictValidator(RowValidator):
    """Validator for district data."""

    area: Area = "district"
    expected_headers = ("code", "regency_code", "name")

    def validate_row(self, row: dict[str, str], row_num: int) -> list[ValidationError]:
        errors: list[ValidationError] = []
        code = row.get("code", "")
        regency_code = row.get("regency_code", "")
        name = row.get("name", "")

        # Validate code (format: NN.NN.NN, e.g., "11.01.01")
        if err := self._validate_not_empty(code, row_num, "code"):
            errors.append(err)
        else:
            if err := self._validate_code_pattern(code, RE_DISTRICT_CODE, "NN.NN.NN", row_num):
                errors.append(err)

        # Validate regency_code (format: NN.NN, e.g., "11.01")
        if err := self._validate_not_empty(regency_code, row_num, "regency_code"):
            errors.append(err)
        else:
            if err := self._validate_code_pattern(
                regency_code, RE_REGENCY_CODE, "NN.NN", row_num, "regency_code"
            ):
                errors.append(err)
            # Validate parent code matches prefix
            elif code and RE_DISTRICT_CODE.match(code):
                if err := self._validate_parent_code_prefix(
                    code, regency_code, REGENCY_CODE_LENGTH, row_num, "regency_code"
                ):
                    errors.append(err)

        # Validate name
        if err := self._validate_not_empty(name, row_num, "name"):
            errors.append(err)

        return errors


class VillageValidator(RowValidator):
    """Validator for village data."""

    area: Area = "village"
    expected_headers = ("code", "district_code", "name")

    def validate_row(self, row: dict[str, str], row_num: int) -> list[ValidationError]:
        errors: list[ValidationError] = []
        code = row.get("code", "")
        district_code = row.get("district_code", "")
        name = row.get("name", "")

        # Validate code (format: NN.NN.NN.NNNN, e.g., "11.01.01.2001")
        if err := self._validate_not_empty(code, row_num, "code"):
            errors.append(err)
        else:
            if err := self._validate_code_pattern(code, RE_VILLAGE_CODE, "NN.NN.NN.NNNN", row_num):
                errors.append(err)

        # Validate district_code (format: NN.NN.NN, e.g., "11.01.01")
        if err := self._validate_not_empty(district_code, row_num, "district_code"):
            errors.append(err)
        else:
            if err := self._validate_code_pattern(
                district_code, RE_DISTRICT_CODE, "NN.NN.NN", row_num, "district_code"
            ):
                errors.append(err)
            # Validate parent code matches prefix
            elif code and RE_VILLAGE_CODE.match(code):
                if err := self._validate_parent_code_prefix(
                    code, district_code, DISTRICT_CODE_LENGTH, row_num, "district_code"
                ):
                    errors.append(err)

        # Validate name
        if err := self._validate_not_empty(name, row_num, "name"):
            errors.append(err)

        return errors


class IslandValidator(RowValidator):
    """Validator for island data."""

    area: Area = "island"
    expected_headers = (
        "code",
        "regency_code",
        "coordinate",
        "is_populated",
        "is_outermost_small",
        "name",
    )

    def validate_row(self, row: dict[str, str], row_num: int) -> list[ValidationError]:
        errors: list[ValidationError] = []
        code = row.get("code", "")
        regency_code = row.get("regency_code", "")
        coordinate = row.get("coordinate", "")
        is_populated = row.get("is_populated", "")
        is_outermost_small = row.get("is_outermost_small", "")
        name = row.get("name", "")

        # Validate code (pattern: NN.NN.NNNNN)
        if err := self._validate_not_empty(code, row_num, "code"):
            errors.append(err)
        elif not RE_ISLAND_CODE.match(code):
            errors.append(
                ValidationError(
                    row_number=row_num,
                    column="code",
                    value=code,
                    error_type="invalid_code_format",
                    message=f"Island code must match pattern NN.NN.NNNNN, got '{code}'",
                )
            )

        # Validate regency_code (can be empty for regency-less islands like NN.00.NNNNN)
        # Format: NN.NN (e.g., "11.01")
        if regency_code:  # Only validate if not empty
            if err := self._validate_code_pattern(
                regency_code, RE_REGENCY_CODE, "NN.NN", row_num, "regency_code"
            ):
                errors.append(err)

        # Validate coordinate
        if err := self._validate_not_empty(coordinate, row_num, "coordinate"):
            errors.append(err)
        elif not RE_COORDINATE.match(coordinate):
            errors.append(
                ValidationError(
                    row_number=row_num,
                    column="coordinate",
                    value=coordinate,
                    error_type="invalid_coordinate",
                    message=(
                        f"Coordinate must match pattern DD°MM'SS.ss\" N DDD°MM'SS.ss\" E, "
                        f"got '{coordinate}'"
                    ),
                )
            )

        # Validate is_populated (must be 0 or 1)
        if err := self._validate_boolean(is_populated, row_num, "is_populated"):
            errors.append(err)

        # Validate is_outermost_small (must be 0 or 1)
        if err := self._validate_boolean(is_outermost_small, row_num, "is_outermost_small"):
            errors.append(err)

        # Validate name
        if err := self._validate_not_empty(name, row_num, "name"):
            errors.append(err)

        return errors


def get_validator(area: Area) -> RowValidator:
    """Factory function to get the appropriate validator for an area type."""
    validators: dict[Area, type[RowValidator]] = {
        "province": ProvinceValidator,
        "regency": RegencyValidator,
        "district": DistrictValidator,
        "village": VillageValidator,
        "island": IslandValidator,
    }
    return validators[area]()


def validate_csv(
    file_path: Path, area: Area, *, chunk_size: int = 1000
) -> Iterator[ValidationReport]:
    """
    Validate a CSV file and yield progress reports.

    Yields intermediate reports for progress tracking, with the final
    report containing complete validation results.

    Args:
        file_path: Path to the CSV file to validate
        area: Area type for validation rules
        chunk_size: Number of rows to process before yielding progress

    Yields:
        ValidationReport with current validation state
    """
    validator = get_validator(area)
    report = ValidationReport(area=area, total_rows=0)

    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Validate headers first
        if reader.fieldnames:
            headers = list(reader.fieldnames)
            if header_error := validator.validate_headers(headers):
                report.add_error(header_error)
                yield report
                return

        rows_in_chunk = 0
        invalid_row_numbers: set[int] = set()

        for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
            report.total_rows += 1
            errors = validator.validate_row(row, row_num)

            if errors:
                for err in errors:
                    report.add_error(err)
                invalid_row_numbers.add(row_num)

            rows_in_chunk += 1
            if rows_in_chunk >= chunk_size:
                report.invalid_rows = len(invalid_row_numbers)
                report.valid_rows = report.total_rows - report.invalid_rows
                yield report
                rows_in_chunk = 0

    # Final report
    report.invalid_rows = len(invalid_row_numbers)
    report.valid_rows = report.total_rows - report.invalid_rows
    yield report
