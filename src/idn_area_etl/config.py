from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast, runtime_checkable


Area = Literal["province", "regency", "district", "village", "island"]
DEFAULT_CONFIG_FILENAME = "idnareaetl.toml"

# --- Models ------------------------------------------------------------


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

        return Config(data=valid_data_config)
