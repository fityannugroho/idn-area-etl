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
    def _get_bundled_config_path(cls) -> Path:
        """Get path to bundled default config file."""
        # Running from source directory - config is next to this module
        source_dir = Path(__file__).parent
        bundled = source_dir / DEFAULT_CONFIG_FILENAME
        if bundled.is_file():
            return bundled

        raise ConfigError(f"Bundled config '{DEFAULT_CONFIG_FILENAME}' not found in package")

    @classmethod
    def _check_cwd_config_exists(cls) -> bool:
        """Check if config exists in cwd (for migration warning)."""
        return (Path.cwd() / DEFAULT_CONFIG_FILENAME).is_file()

    @classmethod
    def load(
        cls,
        config_path: Path | str | None = None,
        *,
        loader: FileLoader = TomlLoader(),
    ) -> Config:
        """Load configuration.

        Args:
            config_path: Path to config file or directory, or None to use bundled default
            loader: File loader strategy

        Returns:
            Parsed Config object

        Raises:
            ConfigError: If config file not found or invalid
        """
        # If config_path is None, load from bundled default
        if config_path is None:
            source_path = cls._get_bundled_config_path()
        else:
            # Convert to Path if string
            if isinstance(config_path, str):
                config_path = Path(config_path)

            # Handle directory vs file
            if config_path.is_dir():
                source_path = config_path / DEFAULT_CONFIG_FILENAME
                if not source_path.is_file():
                    raise ConfigError(
                        f"Config file '{DEFAULT_CONFIG_FILENAME}' not found "
                        f"in directory: {config_path}"
                    )
            elif config_path.is_file():
                source_path = config_path
            else:
                raise ConfigError(f"Configuration path does not exist: {config_path}")

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

        # Parse area extractor config - REQUIRE it to exist
        area_config_raw = extractors_raw.get("area")
        if not isinstance(area_config_raw, dict) or not area_config_raw:
            raise ConfigError("Missing required section [extractors.area] in configuration")

        extractors["area"] = cls._parse_extractor_config(cast(dict[str, Any], area_config_raw))

        # Parse island extractor config - REQUIRE it to exist
        island_config_raw = extractors_raw.get("island")
        if not isinstance(island_config_raw, dict) or not island_config_raw:
            raise ConfigError("Missing required section [extractors.island] in configuration")

        extractors["island"] = cls._parse_extractor_config(cast(dict[str, Any], island_config_raw))

        return Config(
            data=valid_data_config,
            extractors=extractors,
            fuzzy_threshold=fuzzy_threshold,
            exclude_threshold=exclude_threshold,
        )

    @classmethod
    def _parse_extractor_config(cls, raw: dict[str, Any]) -> ExtractorConfig:
        """Parse extractor configuration."""

        def to_tuple(value: Any) -> tuple[str, ...]:
            if isinstance(value, str):
                return tuple(s.strip().lower() for s in value.split(",") if s.strip())
            elif isinstance(value, (list, tuple)):
                items = cast(Iterable[object], value)
                return tuple(str(s).strip().lower() for s in items if str(s).strip())
            return ()

        return ExtractorConfig(
            code_keywords=to_tuple(raw.get("code_keywords")),
            name_keywords=to_tuple(raw.get("name_keywords")),
            coordinate_keywords=to_tuple(raw.get("coordinate_keywords")),
            status_keywords=to_tuple(raw.get("status_keywords")),
            info_keywords=to_tuple(raw.get("info_keywords")),
            exclude_keywords=to_tuple(raw.get("exclude_keywords")),
        )
