"""Tests for remote ground truth management."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from idn_area_etl.remote import (
    CacheError,
    NetworkError,
    RemoteError,
    get_cached_version,
    get_default_ground_truth_path,
    show_version_info,
)


class TestGetDefaultGroundTruth:
    """Tests for get_default_ground_truth_path main function."""

    def test_uses_cache_when_valid(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test using valid cache without downloading."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create valid cache with metadata
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata = {
            "version": "v4.0.0",
            "release_date": "2025-08-15T12:00:00Z",
            "download_date": datetime.now().isoformat(),
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        # Mock network calls (should not be called)
        mock_get_release = mocker.patch("idn_area_etl.remote._get_latest_release_info")

        result = get_default_ground_truth_path()

        assert result == cache_dir
        mock_get_release.assert_not_called()

    def test_downloads_when_cache_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test downloading when no cache exists."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock release info
        mock_get_release = mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH")

        mock_download_fn = mocker.patch(
            "idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download
        )

        result = get_default_ground_truth_path()

        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        assert result == cache_dir
        assert (cache_dir / "provinces.csv").exists()
        mock_get_release.assert_called_once()
        mock_download_fn.assert_called_once()

    def test_downloads_when_cache_outdated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test downloading when cache is outdated."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create outdated cache
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        old_date = (datetime.now() - timedelta(days=10)).isoformat()
        metadata = {
            "version": "v3.0.0",
            "release_date": "2025-07-01T12:00:00Z",
            "download_date": old_date,
        }
        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        # Mock release info (newer version)
        mock_get_release = mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH_NEW")

        mock_download_fn = mocker.patch(
            "idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download
        )

        result = get_default_ground_truth_path()

        assert result == cache_dir
        mock_get_release.assert_called_once()
        mock_download_fn.assert_called_once()

    def test_falls_back_to_cache_on_network_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        capsys: pytest.CaptureFixture[str],
    ):
        """Test using cached data when network fails."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create outdated cache
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        old_date = (datetime.now() - timedelta(days=10)).isoformat()
        metadata = {
            "version": "v3.0.0",
            "release_date": "2025-07-01T12:00:00Z",
            "download_date": old_date,
        }
        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        # Mock network error
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            side_effect=NetworkError("Connection failed"),
        )

        result = get_default_ground_truth_path()

        assert result == cache_dir
        captured = capsys.readouterr()
        assert "Warning:" in captured.out
        assert "Using cached ground truth" in captured.out

    def test_raises_when_no_cache_and_network_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test error when no cache and network fails."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock network error
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            side_effect=NetworkError("Connection failed"),
        )

        with pytest.raises(RemoteError, match="Unable to download ground truth data"):
            get_default_ground_truth_path()

    def test_refresh_cache_forces_download(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test that refresh_cache=True forces download even with valid cache."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create valid cache
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata = {
            "version": "v4.0.0",
            "release_date": "2025-08-15T12:00:00Z",
            "download_date": datetime.now().isoformat(),
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        # Mock release info
        mock_get_release = mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH_REFRESHED")

        mock_download_fn = mocker.patch(
            "idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download
        )

        result = get_default_ground_truth_path(refresh_cache=True)

        assert result == cache_dir
        mock_get_release.assert_called_once()
        mock_download_fn.assert_called_once()


class TestVersionInfo:
    """Tests for version information display."""

    def test_get_cached_version_returns_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test retrieving cached version."""
        monkeypatch.setenv("HOME", str(tmp_path))

        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "version": "v4.0.0",
            "release_date": "2025-08-15T12:00:00Z",
            "download_date": datetime.now().isoformat(),
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        version = get_cached_version()

        assert version == "v4.0.0"

    def test_get_cached_version_returns_none_when_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test get_cached_version returns None when no cache."""
        monkeypatch.setenv("HOME", str(tmp_path))

        version = get_cached_version()

        assert version is None

    def test_show_version_info_displays_metadata(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ):
        """Test version info display."""
        monkeypatch.setenv("HOME", str(tmp_path))

        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "version": "v4.0.0",
            "release_date": "2025-08-15T12:00:00Z",
            "download_date": datetime.now().isoformat(),
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        show_version_info()

        captured = capsys.readouterr()
        assert "v4.0.0" in captured.out
        assert "2025-08-15" in captured.out
        assert "Valid" in captured.out or "Outdated" in captured.out

    def test_show_version_info_no_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ):
        """Test version info when no cache exists."""
        monkeypatch.setenv("HOME", str(tmp_path))

        show_version_info()

        captured = capsys.readouterr()
        assert "No cached ground truth data found" in captured.out


class TestCorruptedMetadata:
    """Tests for handling corrupted cache metadata."""

    def test_get_cached_version_with_corrupted_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test handling corrupted JSON metadata file."""
        monkeypatch.setenv("HOME", str(tmp_path))

        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text("{ invalid json content")

        version = get_cached_version()

        assert version is None

    def test_get_cached_version_with_ioerror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling IOError when reading metadata."""
        monkeypatch.setenv("HOME", str(tmp_path))

        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text('{"version": "v4.0.0"}')

        # Mock open to raise IOError
        mock_open = mocker.mock_open()
        mock_open.side_effect = IOError("Permission denied")
        mocker.patch("pathlib.Path.open", mock_open)

        version = get_cached_version()

        assert version is None

    def test_cache_invalid_with_corrupted_metadata(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test cache is considered invalid when metadata is corrupted."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create cache with CSV files
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        # Create corrupted metadata
        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.write_text("{ invalid json")

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH_NEW")

        mocker.patch("idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download)

        # Should trigger download because cache is invalid
        result = get_default_ground_truth_path()

        assert result == cache_dir

    def test_cache_invalid_with_missing_download_date_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test cache is invalid when download_date key is missing."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create cache with CSV files
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        # Create metadata without download_date
        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata = {"version": "v4.0.0", "release_date": "2025-08-15T12:00:00Z"}
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH_NEW")

        mocker.patch("idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download)

        # Should trigger download because cache is invalid
        result = get_default_ground_truth_path()

        assert result == cache_dir

    def test_cache_invalid_with_malformed_download_date(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test cache is invalid when download_date has invalid format."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create cache with CSV files
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        # Create metadata with malformed date
        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata = {
            "version": "v4.0.0",
            "release_date": "2025-08-15T12:00:00Z",
            "download_date": "not-a-valid-date",
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH_NEW")

        mocker.patch("idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download)

        # Should trigger download because cache is invalid
        result = get_default_ground_truth_path()

        assert result == cache_dir

    def test_show_version_info_with_corrupted_download_date(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ):
        """Test show_version_info handles corrupted download_date gracefully."""
        monkeypatch.setenv("HOME", str(tmp_path))

        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "version": "v4.0.0",
            "release_date": "2025-08-15T12:00:00Z",
            "download_date": "invalid-date-format",
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        show_version_info()

        captured = capsys.readouterr()
        assert "v4.0.0" in captured.out
        assert "Status: Unknown" in captured.out

    def test_show_version_info_with_missing_download_date_key(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ):
        """Test show_version_info handles missing download_date key."""
        monkeypatch.setenv("HOME", str(tmp_path))

        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {"version": "v4.0.0", "release_date": "2025-08-15T12:00:00Z"}
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        show_version_info()

        captured = capsys.readouterr()
        assert "v4.0.0" in captured.out
        assert "Status: Unknown" in captured.out
        assert "Status: Unknown" in captured.out


class TestNetworkErrors:
    """Tests for network error handling."""

    def test_get_default_with_http_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling HTTP errors from GitHub API."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock HTTP error

        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            side_effect=NetworkError("HTTPError: 404 Not Found"),
        )

        with pytest.raises(RemoteError, match="Unable to download ground truth data"):
            get_default_ground_truth_path()

    def test_get_default_with_invalid_json_response(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling invalid JSON response from GitHub API."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock invalid JSON error
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            side_effect=NetworkError("Invalid response from GitHub API: JSONDecodeError"),
        )

        with pytest.raises(RemoteError, match="Unable to download ground truth data"):
            get_default_ground_truth_path()

    def test_get_default_with_timeout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling timeout errors."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock timeout error
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            side_effect=NetworkError("TimeoutError: Request timed out"),
        )

        with pytest.raises(RemoteError, match="Unable to download ground truth data"):
            get_default_ground_truth_path()


class TestGitHubToken:
    """Tests for GITHUB_TOKEN environment variable handling."""

    def test_github_token_included_in_headers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test that GITHUB_TOKEN is included in API headers when set."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("GITHUB_TOKEN", "test_token_12345")

        # Mock httpx.Client to capture headers
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "tag_name": "v4.0.0",
            "published_at": "2025-08-15T12:00:00Z",
            "zipball_url": "http://test.url/zipball",
        }
        mock_response.raise_for_status = mocker.Mock()

        mock_client = mocker.Mock()
        mock_client.__enter__ = mocker.Mock(return_value=mock_client)
        mock_client.__exit__ = mocker.Mock()
        mock_client.get = mocker.Mock(return_value=mock_response)

        mocker.patch("httpx.Client", return_value=mock_client)

        # Mock download
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH")

        mocker.patch("idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download)

        _  = get_default_ground_truth_path()

        # Verify Authorization header was included
        call_args = mock_client.get.call_args
        headers = call_args[1]["headers"]
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test_token_12345"

    def test_github_token_not_included_when_absent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test that no Authorization header is sent when GITHUB_TOKEN is not set."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

        # Mock httpx.Client to capture headers
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "tag_name": "v4.0.0",
            "published_at": "2025-08-15T12:00:00Z",
            "zipball_url": "http://test.url/zipball",
        }
        mock_response.raise_for_status = mocker.Mock()

        mock_client = mocker.Mock()
        mock_client.__enter__ = mocker.Mock(return_value=mock_client)
        mock_client.__exit__ = mocker.Mock()
        mock_client.get = mocker.Mock(return_value=mock_response)

        mocker.patch("httpx.Client", return_value=mock_client)

        # Mock download
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH")

        mocker.patch("idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download)

        _  = get_default_ground_truth_path()

        # Verify Authorization header was NOT included
        call_args = mock_client.get.call_args
        headers = call_args[1]["headers"]
        assert "Authorization" not in headers


class TestDownloadFailures:
    """Tests for download and extraction failures."""

    def test_download_raises_cache_error_on_bad_zip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling bad zip file during download."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download to raise CacheError

        mocker.patch(
            "idn_area_etl.remote._download_and_extract_zipball",
            side_effect=CacheError("Invalid zip file: BadZipFile"),
        )

        with pytest.raises(RemoteError, match="Failed to prepare ground truth cache"):
            get_default_ground_truth_path()

    def test_download_raises_cache_error_on_missing_csv_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling missing CSV files in zipball."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download to raise CacheError

        mocker.patch(
            "idn_area_etl.remote._download_and_extract_zipball",
            side_effect=CacheError("No CSV files found in data/ folder of the release archive"),
        )

        with pytest.raises(RemoteError, match="Failed to prepare ground truth cache"):
            get_default_ground_truth_path()

    def test_download_raises_cache_error_on_io_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling IOError during extraction."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download to raise CacheError

        mocker.patch(
            "idn_area_etl.remote._download_and_extract_zipball",
            side_effect=CacheError("Failed to extract files: IOError"),
        )

        with pytest.raises(RemoteError, match="Failed to prepare ground truth cache"):
            get_default_ground_truth_path()


class TestCacheVersionMatching:
    """Tests for cache version matching scenario."""

    def test_skips_download_when_already_have_latest_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test skipping download when cache already has latest version."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create outdated cache (older than 7 days)
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        old_date = (datetime.now() - timedelta(days=10)).isoformat()
        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata = {
            "version": "v4.0.0",
            "release_date": "2025-08-15T12:00:00Z",
            "download_date": old_date,
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        # Mock release info (same version as cache)
        mock_get_release = mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download (should NOT be called)
        mock_download = mocker.patch("idn_area_etl.remote._download_and_extract_zipball")

        result = get_default_ground_truth_path()

        assert result == cache_dir
        mock_get_release.assert_called_once()
        mock_download.assert_not_called()  # Should NOT download

        # Verify metadata timestamp was updated
        with metadata_path.open("r") as f:
            updated_metadata = json.load(f)
        assert updated_metadata["version"] == "v4.0.0"
        # download_date should be updated to current time
        updated_date = datetime.fromisoformat(updated_metadata["download_date"])
        assert (datetime.now() - updated_date).total_seconds() < 10  # Within 10 seconds

    def test_downloads_when_newer_version_available(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test downloading when newer version is available."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create cache with old version
        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "provinces.csv").write_text("code,name\n11,ACEH")

        old_date = (datetime.now() - timedelta(days=10)).isoformat()
        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata = {
            "version": "v3.0.0",  # Older version
            "release_date": "2025-07-01T12:00:00Z",
            "download_date": old_date,
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        # Mock release info (newer version)
        mock_get_release = mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",  # Newer version
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock download (SHOULD be called)
        def mock_download(url: str, target: Path, *, show_progress: bool = True):
            target.mkdir(parents=True, exist_ok=True)
            (target / "provinces.csv").write_text("code,name\n11,ACEH_NEW")

        mock_download_fn = mocker.patch(
            "idn_area_etl.remote._download_and_extract_zipball", side_effect=mock_download
        )

        result = get_default_ground_truth_path()

        assert result == cache_dir
        mock_get_release.assert_called_once()
        mock_download_fn.assert_called_once()  # SHOULD download


class TestProgressBar:
    """Tests for progress bar display during downloads."""

    def test_download_shows_progress_bar_by_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test that download shows progress bar by default."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create temp zipfile with CSV data
        import zipfile

        temp_zip = tmp_path / "test.zip"
        with zipfile.ZipFile(temp_zip, "w") as zf:
            zf.writestr("idn-area-data-abc123/data/provinces.csv", "code,name\n11,ACEH")
            zf.writestr("idn-area-data-abc123/data/regencies.csv", "code,name\n1101,SIMEULUE")

        # Mock httpx.stream to return our test zip
        mock_response = mocker.Mock()
        mock_response.raise_for_status = mocker.Mock()
        mock_response.headers = {"content-length": str(temp_zip.stat().st_size)}

        # Read zip file and yield chunks
        def iter_bytes(chunk_size: int = 8192):
            with temp_zip.open("rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        mock_response.iter_bytes = iter_bytes
        mock_response.__enter__ = mocker.Mock(return_value=mock_response)
        mock_response.__exit__ = mocker.Mock()

        mocker.patch("httpx.stream", return_value=mock_response)

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        result = get_default_ground_truth_path()

        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        assert result == cache_dir
        assert (cache_dir / "provinces.csv").exists()
        assert (cache_dir / "regencies.csv").exists()


class TestRealAPIErrors:
    """Tests for real GitHub API error scenarios."""

    def test_handles_github_api_missing_keys(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling GitHub API response missing required keys."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock _get_latest_release_info to raise NetworkError
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            side_effect=NetworkError("Invalid response from GitHub API: KeyError"),
        )

        with pytest.raises(RemoteError, match="Unable to download ground truth data"):
            get_default_ground_truth_path()

    def test_handles_json_decode_error_from_api(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling JSONDecodeError from GitHub API."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock _get_latest_release_info to raise NetworkError
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            side_effect=NetworkError("Invalid response from GitHub API: JSONDecodeError"),
        )

        with pytest.raises(RemoteError, match="Unable to download ground truth data"):
            get_default_ground_truth_path()

    def test_handles_http_status_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling HTTP status errors from GitHub API."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock _get_latest_release_info to raise NetworkError
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            side_effect=NetworkError("Failed to fetch latest release info: HTTPStatusError"),
        )

        with pytest.raises(RemoteError, match="Unable to download ground truth data"):
            get_default_ground_truth_path()


class TestDownloadRealErrors:
    """Tests for real download and extraction error scenarios."""

    def test_handles_download_http_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling HTTP error during download."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        # Mock _download_and_extract_zipball to raise NetworkError
        mocker.patch(
            "idn_area_etl.remote._download_and_extract_zipball",
            side_effect=NetworkError("Failed to download zipball: HTTPStatusError"),
        )

        with pytest.raises(RemoteError, match="Unable to download ground truth data"):
            get_default_ground_truth_path()

    def test_handles_bad_zipfile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling bad zipfile during extraction."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create invalid zip file
        temp_zip = tmp_path / "bad.zip"
        temp_zip.write_text("This is not a valid zip file")

        # Mock httpx.stream to return bad zip
        mock_response = mocker.Mock()
        mock_response.raise_for_status = mocker.Mock()
        mock_response.headers = {"content-length": str(temp_zip.stat().st_size)}

        def iter_bytes(chunk_size: int = 8192):
            with temp_zip.open("rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        mock_response.iter_bytes = iter_bytes
        mock_response.__enter__ = mocker.Mock(return_value=mock_response)
        mock_response.__exit__ = mocker.Mock()

        mocker.patch("httpx.stream", return_value=mock_response)

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        with pytest.raises(RemoteError, match="Failed to prepare ground truth cache"):
            get_default_ground_truth_path()

    def test_handles_zipfile_without_csv_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test handling zipfile without CSV files in data/ folder."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create zipfile without data/ folder
        import zipfile

        temp_zip = tmp_path / "nodata.zip"
        with zipfile.ZipFile(temp_zip, "w") as zf:
            zf.writestr("idn-area-data-abc123/README.md", "# README")
            zf.writestr("idn-area-data-abc123/other/file.txt", "test")

        # Mock httpx.stream to return our test zip
        mock_response = mocker.Mock()
        mock_response.raise_for_status = mocker.Mock()
        mock_response.headers = {"content-length": str(temp_zip.stat().st_size)}

        def iter_bytes(chunk_size: int = 8192):
            with temp_zip.open("rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        mock_response.iter_bytes = iter_bytes
        mock_response.__enter__ = mocker.Mock(return_value=mock_response)
        mock_response.__exit__ = mocker.Mock()

        mocker.patch("httpx.stream", return_value=mock_response)

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        with pytest.raises(RemoteError, match="Failed to prepare ground truth cache"):
            get_default_ground_truth_path()

    def test_handles_download_without_content_length(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ):
        """Test download without content-length header (no progress bar)."""
        monkeypatch.setenv("HOME", str(tmp_path))

        # Create temp zipfile with CSV data
        import zipfile

        temp_zip = tmp_path / "test.zip"
        with zipfile.ZipFile(temp_zip, "w") as zf:
            zf.writestr("idn-area-data-abc123/data/provinces.csv", "code,name\n11,ACEH")

        # Mock httpx.stream without content-length
        mock_response = mocker.Mock()
        mock_response.raise_for_status = mocker.Mock()
        mock_response.headers = {}  # No content-length

        def iter_bytes(chunk_size: int = 8192):
            with temp_zip.open("rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        mock_response.iter_bytes = iter_bytes
        mock_response.__enter__ = mocker.Mock(return_value=mock_response)
        mock_response.__exit__ = mocker.Mock()

        mocker.patch("httpx.stream", return_value=mock_response)

        # Mock release info
        mocker.patch(
            "idn_area_etl.remote._get_latest_release_info",
            return_value={
                "tag_name": "v4.0.0",
                "published_at": "2025-08-15T12:00:00Z",
                "zipball_url": "http://test.url/zipball",
            },
        )

        result = get_default_ground_truth_path()

        cache_dir = tmp_path / ".cache" / "idn-area-etl" / "ground-truth"
        assert result == cache_dir
        assert (cache_dir / "provinces.csv").exists()


class TestShowVersionInfoEdgeCases:
    """Additional edge cases for show_version_info."""

    def test_show_version_info_with_outdated_cache(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ):
        """Test show_version_info displays outdated status correctly."""
        monkeypatch.setenv("HOME", str(tmp_path))

        old_date = (datetime.now() - timedelta(days=10)).isoformat()
        metadata_path = tmp_path / ".cache" / "idn-area-etl" / "metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "version": "v4.0.0",
            "release_date": "2025-08-15T12:00:00Z",
            "download_date": old_date,
        }
        with metadata_path.open("w") as f:
            json.dump(metadata, f)

        show_version_info()

        captured = capsys.readouterr()
        assert "v4.0.0" in captured.out
        assert "Outdated" in captured.out
