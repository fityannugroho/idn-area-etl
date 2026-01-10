from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast, runtime_checkable

Area = Literal["province", "regency", "district", "village", "island"]
DEFAULT_CONFIG_FILENAME = "idnareaetl.toml"

# --- Models ------------------------------------------------------------


@dataclass
class ExtractorConfig:
    """Configuration for extractor keyword matching."""

    code_keywords: tuple[str, ...] = ()
    name_keywords: tuple[str, ...] = ()
    coordinate_keywords: tuple[str, ...] = ()
    status_keywords: tuple[str, ...] = ()
    info_keywords: tuple[str, ...] = ()
    exclude_keywords: tuple[str, ...] = ()


# Default extractor configurations
DEFAULT_AREA_CONFIG = ExtractorConfig(
    code_keywords=("kode",),
    name_keywords=("nama", "provinsi", "kabupaten", "kota", "kecamatan", "desa", "kelurahan"),
    exclude_keywords=("no", "ibukota", "jumlah penduduk", "penduduk", "ibu kota"),
)

DEFAULT_ISLAND_CONFIG = ExtractorConfig(
    code_keywords=("kode", "pulau"),
    name_keywords=("nama", "pulau"),
    coordinate_keywords=("koordinat", "kordinat"),
    status_keywords=("bp/tbp", "bp", "tbp", "status", "keterangan"),
    info_keywords=("keterangan", "ket"),
    exclude_keywords=("no", "ibukota", "jumlah penduduk", "penduduk", "ibu kota"),
)


@dataclass
class DataConfig:
    batch_size: int
    output_headers: tuple[str, ...]
    filename_suffix: str

    def __post_init__(self):
        if self.batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")

        if not self.filename_suffix:
            raise ValueError("filename_suffix must be a non-empty string")

        if not self.output_headers:
            raise ValueError("expected_headers must be a non-empty tuple")


@dataclass
class Config:
    """Application configuration loaded from TOML file."""

    data: dict[Area, DataConfig]
    extractors: dict[str, ExtractorConfig]
    fuzzy_threshold: float = 80.0
    exclude_threshold: float = 65.0


# --- Errors -----------------------------------------------------------------


class ConfigError(Exception):
    """Raised when the TOML configuration is missing or invalid."""


class UnsupportedFormatError(ConfigError):
    pass


class ParseError(ConfigError):
    pass


# --- File loader abstraction (Strategy) ------------------------------------------


@runtime_checkable
class FileLoader(Protocol):
    """Protocol for file loaders that load a file from a given path."""

    def load(self, path: Path) -> dict[str, Any]: ...


# --- Concrete loaders --------------------------------------------------------


class TomlLoader:
    """Loader for TOML files."""

    def load(self, path: Path) -> dict[str, Any]:
        import tomllib

        with path.open("rb") as f:
            return tomllib.load(f)


class AppConfig:
    """Application configuration manager."""

    @classmethod
    def load(
        cls,
        source_path: Path = Path.cwd() / DEFAULT_CONFIG_FILENAME,
        *,
        loader: FileLoader = TomlLoader(),
    ) -> Config:
        """Load configuration. If source_path is None, load from the default location."""
        if not source_path.is_file():
            raise ConfigError(f"Configuration file not found: {source_path}")

        try:
            raw = loader.load(source_path)
        except Exception as e:
            raise ConfigError(e)

        return cls._parse(raw)

    @classmethod
    def _parse(cls, raw: dict[str, Any]) -> Config:
        """Parse raw configuration data."""
        data = raw.get("data")

        if not isinstance(data, dict) or not data:
            raise ConfigError("Configuration must contain a non-empty 'data' table")

        data = cast(dict[Any, Any], data)
        valid_data_config: dict[Area, DataConfig] = {}

        for raw_area, raw_data_config in data.items():
            if not isinstance(raw_data_config, dict):
                raise ConfigError(f"Missing or invalid configuration for area '{raw_area}'")

            raw_data_config = cast(dict[str, Any], raw_data_config)

            try:
                batch_size = int(raw_data_config.get("batch_size", 0))
                raw_output_headers = raw_data_config.get("output_headers", ())

                headers_iterable: Iterable[str]

                if isinstance(raw_output_headers, str):
                    headers_iterable = (header.strip() for header in raw_output_headers.split(","))
                elif isinstance(raw_output_headers, (list, tuple, set)):
                    headers_iterable = (
                        str(header).strip() for header in cast(Iterable[object], raw_output_headers)
                    )
                elif raw_output_headers is None:
                    headers_iterable = ()
                else:
                    raise ConfigError("output_headers must be a string or a sequence of strings")

                output_headers = tuple(h for h in headers_iterable if h)
                filename_suffix = str(
                    raw_data_config.get("filename_suffix", f"_{raw_area}.csv")
                ).strip()

                data_config = DataConfig(
                    batch_size=batch_size,
                    output_headers=output_headers,
                    filename_suffix=filename_suffix,
                )
                valid_data_config[raw_area] = data_config
            except (ValueError, TypeError) as e:
                raise ConfigError(e) from e

        # Parse extractors configuration with defaults
        extractors_raw = raw.get("extractors", {})
        if not isinstance(extractors_raw, dict):
            extractors_raw = {}

        extractors_raw = cast(dict[str, Any], extractors_raw)

        # Get global thresholds
        fuzzy_threshold = float(extractors_raw.get("fuzzy_threshold", 80.0))
        exclude_threshold = float(extractors_raw.get("exclude_threshold", 65.0))

        # Parse area and island extractor configs
        extractors: dict[str, ExtractorConfig] = {}

        # Parse area extractor config
        area_config_raw = extractors_raw.get("area", {})
        if isinstance(area_config_raw, dict):
            extractors["area"] = cls._parse_extractor_config(
                cast(dict[str, Any], area_config_raw), DEFAULT_AREA_CONFIG
            )
        else:
            extractors["area"] = DEFAULT_AREA_CONFIG

        # Parse island extractor config
        island_config_raw = extractors_raw.get("island", {})
        if isinstance(island_config_raw, dict):
            extractors["island"] = cls._parse_extractor_config(
                cast(dict[str, Any], island_config_raw), DEFAULT_ISLAND_CONFIG
            )
        else:
            extractors["island"] = DEFAULT_ISLAND_CONFIG

        return Config(
            data=valid_data_config,
            extractors=extractors,
            fuzzy_threshold=fuzzy_threshold,
            exclude_threshold=exclude_threshold,
        )

    @classmethod
    def _parse_extractor_config(
        cls, raw: dict[str, Any], default: ExtractorConfig
    ) -> ExtractorConfig:
        """Parse extractor configuration with defaults."""

        def to_tuple(value: Any, default: tuple[str, ...]) -> tuple[str, ...]:
            if isinstance(value, str):
                return tuple(s.strip().lower() for s in value.split(",") if s.strip())
            elif isinstance(value, (list, tuple)):
                items = cast(Iterable[object], value)
                return tuple(str(s).strip().lower() for s in items if str(s).strip())
            return default

        return ExtractorConfig(
            code_keywords=to_tuple(raw.get("code_keywords"), default.code_keywords),
            name_keywords=to_tuple(raw.get("name_keywords"), default.name_keywords),
            coordinate_keywords=to_tuple(
                raw.get("coordinate_keywords"), default.coordinate_keywords
            ),
            status_keywords=to_tuple(raw.get("status_keywords"), default.status_keywords),
            info_keywords=to_tuple(raw.get("info_keywords"), default.info_keywords),
            exclude_keywords=to_tuple(raw.get("exclude_keywords"), default.exclude_keywords),
        )
