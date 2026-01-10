from pathlib import Path
from typing import Any

import pytest

from idn_area_etl.config import AppConfig, ConfigError, DataConfig, FileLoader


class StubLoader(FileLoader):
    def __init__(
        self,
        *,
        payload: dict[str, Any] | None = None,
        exception: Exception | None = None,
    ) -> None:
        self.payload: dict[str, Any] = payload or {}
        self.exception = exception

    def load(self, path: Path) -> dict[str, Any]:
        if self.exception is not None:
            raise self.exception
        return self.payload


def _touch_config(tmp_path: Path) -> Path:
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("# stub config", encoding="utf-8")
    return cfg_path


def _base_area_config(**overrides: object) -> dict[str, Any]:
    data: dict[str, Any] = {
        "batch_size": 8,
        "output_headers": ["code", "name"],
        "filename_suffix": "province",
    }
    data.update(overrides)
    return data


class TestDataConfigValidation:
    def test_requires_positive_batch_size(self) -> None:
        with pytest.raises(ValueError):
            DataConfig(batch_size=0, output_headers=("code",), filename_suffix="province")

    def test_requires_non_empty_filename_suffix(self) -> None:
        with pytest.raises(ValueError):
            DataConfig(batch_size=1, output_headers=("code",), filename_suffix="")

    def test_requires_non_empty_headers(self) -> None:
        with pytest.raises(ValueError):
            DataConfig(batch_size=1, output_headers=(), filename_suffix="province")


class TestAppConfigLoad:
    def test_load_bundled_default_when_none(self) -> None:
        """Test loading with None loads bundled config."""
        # Should load from bundled location without error
        cfg = AppConfig.load(None)
        # Verify it loaded valid config data
        assert "province" in cfg.data
        assert "island" in cfg.data

    def test_load_from_directory_with_config_file(self, tmp_path: Path) -> None:
        """Test loading from directory path."""
        from idn_area_etl.config import DEFAULT_CONFIG_FILENAME

        cfg_file = tmp_path / DEFAULT_CONFIG_FILENAME
        cfg_file.write_text("""
[data.province]
batch_size = 5
output_headers = ["code", "name"]
filename_suffix = "test_province"

[extractors.area]
code_keywords = ["kode"]
name_keywords = ["nama"]

[extractors.island]
code_keywords = ["kode"]
name_keywords = ["nama"]
""")

        cfg = AppConfig.load(tmp_path)
        assert cfg.data["province"].batch_size == 5
        assert cfg.data["province"].filename_suffix == "test_province"

    def test_load_from_directory_without_config_raises_error(self, tmp_path: Path) -> None:
        """Test error when dir missing config."""
        with pytest.raises(ConfigError) as exc_info:
            AppConfig.load(tmp_path)
        assert "not found in directory" in str(exc_info.value)

    def test_load_from_nonexistent_path_raises_error(self, tmp_path: Path) -> None:
        """Test error for invalid path."""
        nonexistent = tmp_path / "does_not_exist.toml"
        with pytest.raises(ConfigError) as exc_info:
            AppConfig.load(nonexistent)
        assert "does not exist" in str(exc_info.value)

    def test_raises_when_file_missing(self, tmp_path: Path) -> None:
        missing = tmp_path / "absent.toml"
        with pytest.raises(ConfigError) as exc_info:
            AppConfig.load(missing)
        assert str(missing) in str(exc_info.value) or "does not exist" in str(exc_info.value)

    def test_wraps_loader_exception(self, tmp_path: Path) -> None:
        cfg_path = _touch_config(tmp_path)
        loader = StubLoader(exception=RuntimeError("boom"))

        with pytest.raises(ConfigError) as exc_info:
            AppConfig.load(cfg_path, loader=loader)
        assert "boom" in str(exc_info.value)

    def test_accepts_string_headers_and_default_suffix(self, tmp_path: Path) -> None:
        cfg_path = _touch_config(tmp_path)
        payload = {
            "data": {
                "province": {
                    "batch_size": "5",
                    "output_headers": "code , name ",
                }
            },
            "extractors": {
                "area": {
                    "code_keywords": ["kode"],
                    "name_keywords": ["nama"],
                },
                "island": {
                    "code_keywords": ["kode"],
                    "name_keywords": ["nama"],
                },
            },
        }

        cfg = AppConfig.load(cfg_path, loader=StubLoader(payload=payload))
        province = cfg.data["province"]
        assert province.batch_size == 5
        assert province.output_headers == ("code", "name")
        assert province.filename_suffix == "_province.csv"

    def test_accepts_iterable_headers(self, tmp_path: Path) -> None:
        cfg_path = _touch_config(tmp_path)
        payload = {
            "data": {
                "island": {
                    "batch_size": 3,
                    "output_headers": ["coordinate", "code", "name"],
                    "filename_suffix": "island",
                }
            },
            "extractors": {
                "area": {
                    "code_keywords": ["kode"],
                    "name_keywords": ["nama"],
                },
                "island": {
                    "code_keywords": ["kode"],
                    "name_keywords": ["nama"],
                },
            },
        }

        cfg = AppConfig.load(cfg_path, loader=StubLoader(payload=payload))
        assert set(cfg.data["island"].output_headers) == {"code", "name", "coordinate"}

    def test_none_headers_raise_config_error(self, tmp_path: Path) -> None:
        cfg_path = _touch_config(tmp_path)
        payload = {"data": {"province": _base_area_config(output_headers=None)}}

        with pytest.raises(ConfigError):
            AppConfig.load(cfg_path, loader=StubLoader(payload=payload))

    def test_invalid_headers_type_raise_config_error(self, tmp_path: Path) -> None:
        cfg_path = _touch_config(tmp_path)
        payload = {"data": {"province": _base_area_config(output_headers=123)}}

        with pytest.raises(ConfigError):
            AppConfig.load(cfg_path, loader=StubLoader(payload=payload))

    def test_invalid_dataconfig_value_is_wrapped(self, tmp_path: Path) -> None:
        cfg_path = _touch_config(tmp_path)
        payload = {"data": {"province": _base_area_config(batch_size=0)}}

        with pytest.raises(ConfigError):
            AppConfig.load(cfg_path, loader=StubLoader(payload=payload))

    def test_requires_data_table(self, tmp_path: Path) -> None:
        cfg_path = _touch_config(tmp_path)

        with pytest.raises(ConfigError):
            AppConfig.load(cfg_path, loader=StubLoader(payload={}))

    def test_requires_dict_per_area(self, tmp_path: Path) -> None:
        cfg_path = _touch_config(tmp_path)
        payload = {"data": {"province": "invalid"}}

        with pytest.raises(ConfigError):
            AppConfig.load(cfg_path, loader=StubLoader(payload=payload))

    def test_requires_extractors_area_section(self, tmp_path: Path) -> None:
        """Test that missing [extractors.area] section raises ConfigError."""
        cfg_path = _touch_config(tmp_path)
        payload = {
            "data": {"province": _base_area_config()},
            "extractors": {
                "island": {
                    "code_keywords": ["kode"],
                    "name_keywords": ["nama"],
                }
                # Missing "area" section
            },
        }

        with pytest.raises(ConfigError) as exc_info:
            AppConfig.load(cfg_path, loader=StubLoader(payload=payload))
        assert "[extractors.area]" in str(exc_info.value)

    def test_requires_extractors_island_section(self, tmp_path: Path) -> None:
        """Test that missing [extractors.island] section raises ConfigError."""
        cfg_path = _touch_config(tmp_path)
        payload = {
            "data": {"province": _base_area_config()},
            "extractors": {
                "area": {
                    "code_keywords": ["kode"],
                    "name_keywords": ["nama"],
                }
                # Missing "island" section
            },
        }

        with pytest.raises(ConfigError) as exc_info:
            AppConfig.load(cfg_path, loader=StubLoader(payload=payload))
        assert "[extractors.island]" in str(exc_info.value)
