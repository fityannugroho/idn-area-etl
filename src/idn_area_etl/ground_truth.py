"""
Ground truth index for Indonesian area data.

Loads reference CSV data and provides lookup/search capabilities for
validating and normalizing extracted data.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Generic, Protocol, TypeVar

from idn_area_etl.config import Area
from idn_area_etl.utils import MatchCandidate, SafeDictReader, fuzzy_search_top_n


class HasCodeAndName(Protocol):
    """Protocol for records that have code and name attributes."""

    code: str
    name: str


T = TypeVar("T", bound=HasCodeAndName)


@dataclass
class AreaRecord:
    """Represents a single area record from ground truth data."""

    code: str
    name: str
    parent_code: str = ""


@dataclass
class IslandRecord:
    """Represents a single island record from ground truth data."""

    code: str
    name: str
    regency_code: str = ""
    coordinate: str = ""
    is_populated: str = ""
    is_outermost_small: str = ""


class BaseIndex(Generic[T]):
    """
    Generic index for records with code and name lookups.

    Provides efficient lookup by code and fuzzy search by name.
    """

    def __init__(self) -> None:
        self.records: list[T] = []
        self._code_to_record: dict[str, T] = {}
        self._names: list[str] = []
        self._codes: list[str] = []

    def add(self, record: T) -> None:
        """Add a record to the index."""
        self.records.append(record)
        self._code_to_record[record.code] = record
        self._names.append(record.name)
        self._codes.append(record.code)

    def get_by_code(self, code: str) -> T | None:
        """Lookup a record by its code."""
        return self._code_to_record.get(code)

    def search_by_name(
        self, name: str, *, n: int = 5, threshold: float = 60.0
    ) -> list[MatchCandidate]:
        """Search for records by name using fuzzy matching."""
        return fuzzy_search_top_n(name, self._names, n=n, threshold=threshold, keys=self._codes)

    def has_code(self, code: str) -> bool:
        """Check if a code exists in the index."""
        return code in self._code_to_record

    def __len__(self) -> int:
        return len(self.records)


class AreaIndex(BaseIndex[AreaRecord]):
    """Index for a single area type with code and name lookups."""

    def __init__(self, area: Area = "province") -> None:
        super().__init__()
        self.area = area


class IslandIndex(BaseIndex[IslandRecord]):
    """Index for island data with code and name lookups."""

    pass


# Mapping constants for parent fields and hierarchy
PARENT_FIELD_MAP: dict[Area, str] = {
    "province": "",
    "regency": "province_code",
    "district": "regency_code",
    "village": "district_code",
    "island": "regency_code",
}


class GroundTruthIndex:
    """
    Hierarchical index for Indonesian area ground truth data.

    Loads CSV files and provides efficient lookup and fuzzy search capabilities
    for provinces, regencies, districts, villages, and islands.
    """

    def __init__(self) -> None:
        self.provinces = AreaIndex(area="province")
        self.regencies = AreaIndex(area="regency")
        self.districts = AreaIndex(area="district")
        self.villages = AreaIndex(area="village")
        self.islands = IslandIndex()

        # Hierarchical mappings for parent-child relationships
        self._regencies_by_province: dict[str, list[AreaRecord]] = {}
        self._districts_by_regency: dict[str, list[AreaRecord]] = {}
        self._villages_by_district: dict[str, list[AreaRecord]] = {}
        self._islands_by_regency: dict[str, list[IslandRecord]] = {}

        # Strategy maps for cleaner lookups
        self._area_indices: dict[Area, AreaIndex] = {
            "province": self.provinces,
            "regency": self.regencies,
            "district": self.districts,
            "village": self.villages,
        }

    def load_from_directory(self, directory: Path) -> None:
        """
        Load all ground truth CSVs from a directory.

        Auto-detects area type from CSV column headers.
        Supports any naming convention as long as files contain the expected columns.

        Args:
            directory: Path to directory containing CSV files
        """
        if not directory.is_dir():
            raise ValueError(f"Directory does not exist: {directory}")

        # Find all CSV files and auto-detect their area type
        for csv_path in sorted(directory.glob("*.csv")):
            area_type = self._detect_area_type_from_headers(csv_path)
            if area_type == "island":
                self._load_island_csv(csv_path)
            elif area_type:
                self._load_area_csv(csv_path, area_type)  # type: ignore[arg-type]

    def _detect_area_type_from_headers(self, csv_path: Path) -> str | None:
        """
        Detect area type from CSV column headers.

        Returns area type ("province", "regency", "district", "village", "island")
        or None if unable to detect.
        """
        try:
            with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
                reader = SafeDictReader(f)
                if not reader.fieldnames:
                    return None

                headers = set(reader.fieldnames)

                # Island: has regency_code AND (coordinate OR is_populated)
                if "regency_code" in headers and (
                    "coordinate" in headers or "is_populated" in headers
                ):
                    return "island"

                # Village: has district_code
                if "district_code" in headers:
                    return "village"

                # District: has regency_code (but not district_code)
                if "regency_code" in headers:
                    return "district"

                # Regency: has province_code
                if "province_code" in headers:
                    return "regency"

                # Province: has code and name, no parent fields
                if "code" in headers and "name" in headers:
                    return "province"

        except (IOError, ValueError):
            pass

        return None

    def _load_area_csv(self, path: Path, area: Area) -> None:
        """Load area CSV into the appropriate index."""
        index = self._get_area_index(area)
        parent_field = PARENT_FIELD_MAP.get(area, "")
        hierarchy_map = self._get_hierarchy_map(area)

        with path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = SafeDictReader(f)
            for row in reader:
                code = row.get("code", "")
                name = row.get("name", "")
                parent_code = row.get(parent_field, "") if parent_field else ""

                record = AreaRecord(code=code, name=name, parent_code=parent_code)
                index.add(record)

                # Add to hierarchy map
                if hierarchy_map is not None and parent_code:
                    if parent_code not in hierarchy_map:
                        hierarchy_map[parent_code] = []
                    hierarchy_map[parent_code].append(record)

    def _load_island_csv(self, path: Path) -> None:
        """Load island CSV into the island index."""
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = SafeDictReader(f)
            for row in reader:
                record = IslandRecord(
                    code=row.get("code", ""),
                    name=row.get("name", ""),
                    regency_code=row.get("regency_code", ""),
                    coordinate=row.get("coordinate", ""),
                    is_populated=row.get("is_populated", ""),
                    is_outermost_small=row.get("is_outermost_small", ""),
                )
                self.islands.add(record)

                # Add to hierarchy
                if record.regency_code:
                    if record.regency_code not in self._islands_by_regency:
                        self._islands_by_regency[record.regency_code] = []
                    self._islands_by_regency[record.regency_code].append(record)

    def _get_area_index(self, area: Area) -> AreaIndex:
        """Get the index for a specific area type."""
        index = self._area_indices.get(area)
        if index is None:
            raise ValueError(f"No index for area type: {area}")
        return index

    def _get_hierarchy_map(self, area: Area) -> dict[str, list[AreaRecord]] | None:
        """Get the hierarchy map for an area type."""
        maps: dict[Area, dict[str, list[AreaRecord]] | None] = {
            "province": None,
            "regency": self._regencies_by_province,
            "district": self._districts_by_regency,
            "village": self._villages_by_district,
            "island": None,  # Islands use a separate map
        }
        return maps.get(area)

    def _get_children_by_parent(
        self, area: Area, parent_code: str
    ) -> list[AreaRecord] | list[IslandRecord]:
        """Get child records for a given parent code based on area type."""
        hierarchy_getters: dict[
            Area, dict[str, list[AreaRecord]] | dict[str, list[IslandRecord]]
        ] = {
            "regency": self._regencies_by_province,
            "district": self._districts_by_regency,
            "village": self._villages_by_district,
            "island": self._islands_by_regency,
        }
        hierarchy = hierarchy_getters.get(area)
        if hierarchy is None:
            return []
        return hierarchy.get(parent_code, [])

    # ===================
    # Public add methods (for programmatic population)
    # ===================

    def add_area_record(self, area: Area, record: AreaRecord) -> None:
        """
        Add an area record to the appropriate index and hierarchy.

        This method is useful for programmatic population of the index
        without loading from CSV files.

        Args:
            area: The area type (province, regency, district, village)
            record: The AreaRecord to add
        """
        index = self._get_area_index(area)
        index.add(record)

        # Populate hierarchy map
        hierarchy_map = self._get_hierarchy_map(area)
        if hierarchy_map is not None and record.parent_code:
            if record.parent_code not in hierarchy_map:
                hierarchy_map[record.parent_code] = []
            hierarchy_map[record.parent_code].append(record)

    def add_island_record(self, record: IslandRecord) -> None:
        """
        Add an island record to the index and hierarchy.

        Args:
            record: The IslandRecord to add
        """
        self.islands.add(record)

        if record.regency_code:
            if record.regency_code not in self._islands_by_regency:
                self._islands_by_regency[record.regency_code] = []
            self._islands_by_regency[record.regency_code].append(record)

    # ===================
    # Query methods
    # ===================

    def get_regencies_for_province(self, province_code: str) -> list[AreaRecord]:
        """Get all regencies belonging to a province."""
        return self._regencies_by_province.get(province_code, [])

    def get_districts_for_regency(self, regency_code: str) -> list[AreaRecord]:
        """Get all districts belonging to a regency."""
        return self._districts_by_regency.get(regency_code, [])

    def get_villages_for_district(self, district_code: str) -> list[AreaRecord]:
        """Get all villages belonging to a district."""
        return self._villages_by_district.get(district_code, [])

    def get_islands_for_regency(self, regency_code: str) -> list[IslandRecord]:
        """Get all islands belonging to a regency."""
        return self._islands_by_regency.get(regency_code, [])

    def search_name_in_context(
        self,
        name: str,
        area: Area,
        parent_code: str | None = None,
        *,
        n: int = 5,
        threshold: float = 60.0,
    ) -> list[MatchCandidate]:
        """
        Search for a name within a specific context (parent area).

        This narrows the search to only records under a specific parent,
        which improves accuracy and reduces false positives.

        Args:
            name: Name to search for
            area: Area type to search in
            parent_code: Optional parent code to narrow the search
            n: Maximum number of results
            threshold: Minimum fuzzy match score

        Returns:
            List of MatchCandidate sorted by score
        """
        # Try context-specific search first
        if parent_code:
            records = self._get_children_by_parent(area, parent_code)
            if records:
                names = [r.name for r in records]
                codes = [r.code for r in records]
                return fuzzy_search_top_n(name, names, n=n, threshold=threshold, keys=codes)

        # Fall back to full index search
        if area == "island":
            return self.islands.search_by_name(name, n=n, threshold=threshold)

        index = self._get_area_index(area)
        return index.search_by_name(name, n=n, threshold=threshold)

    def summary(self) -> str:
        """Return a summary of loaded data."""
        return (
            f"Ground Truth Index:\n"
            f"  Provinces: {len(self.provinces)}\n"
            f"  Regencies: {len(self.regencies)}\n"
            f"  Districts: {len(self.districts)}\n"
            f"  Villages: {len(self.villages)}\n"
            f"  Islands: {len(self.islands)}"
        )
