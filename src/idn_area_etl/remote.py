"""
Remote ground truth data management.

Handles downloading, caching, and updating ground truth CSV files from
the remote repository: https://github.com/fityannugroho/idn-area-data
"""

import json
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from tqdm import tqdm

GITHUB_API_BASE = "https://api.github.com"
REPO_OWNER = "fityannugroho"
REPO_NAME = "idn-area-data"
CACHE_VALIDITY_DAYS = 7
DEFAULT_TIMEOUT = 30.0


class RemoteError(Exception):
    """Base exception for remote operations."""


class NetworkError(RemoteError):
    """Network-related errors."""


class CacheError(RemoteError):
    """Cache-related errors."""


def _get_cache_directory() -> Path:
    """
    Get the cache directory path for ground truth data.

    Returns:
        Path to ~/.cache/idn-area-etl/ground-truth/

    Creates the directory if it doesn't exist.
    """
    cache_dir = Path.home() / ".cache" / "idn-area-etl" / "ground-truth"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _get_cache_metadata_path() -> Path:
    """
    Get the path to cache metadata file.

    Returns:
        Path to ~/.cache/idn-area-etl/metadata.json
    """
    return Path.home() / ".cache" / "idn-area-etl" / "metadata.json"


def _load_cache_metadata() -> dict[str, str] | None:
    """
    Load cache metadata from JSON file.

    Returns:
        Dictionary with keys: version, release_date, download_date
        None if metadata file doesn't exist or is invalid
    """
    metadata_path = _get_cache_metadata_path()
    if not metadata_path.exists():
        return None

    try:
        with metadata_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def _save_cache_metadata(version: str, release_date: str) -> None:
    """
    Save cache metadata to JSON file.

    Args:
        version: Release version tag (e.g., "v4.0.0")
        release_date: ISO 8601 release date from GitHub
    """
    metadata = {
        "version": version,
        "release_date": release_date,
        "download_date": datetime.now().isoformat(),
    }

    metadata_path = _get_cache_metadata_path()
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)


def _is_cache_valid() -> bool:
    """
    Check if the cache is valid (exists and is less than CACHE_VALIDITY_DAYS old).

    Returns:
        True if cache exists and is fresh, False otherwise
    """
    cache_dir = _get_cache_directory()
    metadata = _load_cache_metadata()

    # Check if cache directory has CSV files
    csv_files = list(cache_dir.glob("*.csv"))
    if not csv_files or not metadata:
        return False

    # Check age of cache
    try:
        download_date = datetime.fromisoformat(metadata["download_date"])
        age = datetime.now() - download_date
        return age < timedelta(days=CACHE_VALIDITY_DAYS)
    except (KeyError, ValueError):
        return False


def _get_github_headers() -> dict[str, str]:
    """
    Get headers for GitHub API requests.

    Includes GITHUB_TOKEN from environment if available for higher rate limits.

    Returns:
        Dictionary of HTTP headers
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    github_token = os.environ.get("GITHUB_TOKEN")
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    return headers


def _get_latest_release_info() -> dict[str, Any]:
    """
    Fetch latest release information from GitHub API.

    Returns:
        Dictionary containing: tag_name, published_at, zipball_url

    Raises:
        NetworkError: If unable to fetch release info
    """
    url = f"{GITHUB_API_BASE}/repos/{REPO_OWNER}/{REPO_NAME}/releases/latest"
    headers = _get_github_headers()

    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, headers=headers, follow_redirects=True)
            response.raise_for_status()
            data = response.json()

            return {
                "tag_name": data["tag_name"],
                "published_at": data["published_at"],
                "zipball_url": data["zipball_url"],
            }
    except httpx.HTTPError as e:
        raise NetworkError(f"Failed to fetch latest release info: {e}")
    except (KeyError, json.JSONDecodeError) as e:
        raise NetworkError(f"Invalid response from GitHub API: {e}")


def _download_and_extract_zipball(
    url: str, target_dir: Path, *, show_progress: bool = True
) -> None:
    """
    Download zipball from GitHub and extract data/*.csv files.

    Args:
        url: GitHub zipball URL
        target_dir: Directory to extract CSV files to
        show_progress: Whether to show progress bar

    Raises:
        NetworkError: If download fails
        CacheError: If extraction fails
    """
    temp_zip = target_dir.parent / "temp_download.zip"
    headers = _get_github_headers()

    try:
        # Download with progress bar
        with httpx.stream(
            "GET", url, headers=headers, timeout=DEFAULT_TIMEOUT, follow_redirects=True
        ) as response:
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            with temp_zip.open("wb") as f:
                if show_progress and total_size > 0:
                    with tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        desc="Downloading ground truth",
                        colour="cyan",
                    ) as pbar:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))
                else:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)

    except httpx.HTTPError as e:
        if temp_zip.exists():
            temp_zip.unlink()
        raise NetworkError(f"Failed to download zipball: {e}")

    # Extract data/*.csv files
    try:
        with zipfile.ZipFile(temp_zip, "r") as zf:
            # Find the data/ folder in the zipball
            # GitHub zipballs have structure: {repo}-{sha}/data/*.csv
            data_files = [
                name for name in zf.namelist() if "/data/" in name and name.endswith(".csv")
            ]

            if not data_files:
                raise CacheError("No CSV files found in data/ folder of the release archive")

            # Clear target directory
            if target_dir.exists():
                shutil.rmtree(target_dir)
            target_dir.mkdir(parents=True, exist_ok=True)

            # Extract CSV files to target directory
            for file_path in data_files:
                # Extract just the filename (e.g., provinces.csv)
                filename = Path(file_path).name
                # Extract to target directory with just the filename
                with zf.open(file_path) as source, (target_dir / filename).open("wb") as target:
                    shutil.copyfileobj(source, target)

    except zipfile.BadZipFile as e:
        raise CacheError(f"Invalid zip file: {e}")
    except (IOError, OSError) as e:
        raise CacheError(f"Failed to extract files: {e}")
    finally:
        # Clean up temp file
        if temp_zip.exists():
            temp_zip.unlink()


def get_cached_version() -> str | None:
    """
    Get the version of cached ground truth data.

    Returns:
        Version string (e.g., "v4.0.0") or None if no cache exists
    """
    metadata = _load_cache_metadata()
    return metadata["version"] if metadata else None


def show_version_info() -> None:
    """Display cached ground truth version information."""
    metadata = _load_cache_metadata()

    if not metadata:
        print("No cached ground truth data found.")
        print("Run normalize command without --ground-truth to download.")
        return

    print("Cached Ground Truth Information:")
    print(f"  Version: {metadata.get('version', 'unknown')}")
    print(f"  Release Date: {metadata.get('release_date', 'unknown')}")
    print(f"  Downloaded: {metadata.get('download_date', 'unknown')}")

    # Check if cache is still valid
    try:
        download_date = datetime.fromisoformat(metadata["download_date"])
        age = datetime.now() - download_date
        days_remaining = CACHE_VALIDITY_DAYS - age.days

        if days_remaining > 0:
            print(f"  Status: Valid (expires in {days_remaining} days)")
        else:
            print("  Status: Outdated (will update on next use)")
    except (KeyError, ValueError):
        print("  Status: Unknown")

    cache_dir = _get_cache_directory()
    print(f"  Cache Location: {cache_dir}")


def get_default_ground_truth_path(*, refresh_cache: bool = False) -> Path:
    """
    Get path to ground truth directory, downloading if necessary.

    This is the main entry point for remote ground truth operations.
    Handles caching, updates, and error recovery.

    Args:
        refresh_cache: If True, force re-download even if cache is valid

    Returns:
        Path to directory containing ground truth CSV files

    Raises:
        RemoteError: If unable to get ground truth data
    """
    cache_dir = _get_cache_directory()

    # Check if cache is valid and not forcing refresh
    if not refresh_cache and _is_cache_valid():
        return cache_dir

    # Need to download/update
    try:
        release_info = _get_latest_release_info()

        # Check if we already have this version
        if not refresh_cache:
            metadata = _load_cache_metadata()
            if metadata and metadata.get("version") == release_info["tag_name"]:
                # Already have latest version, just update the download timestamp
                _save_cache_metadata(release_info["tag_name"], release_info["published_at"])
                return cache_dir

        # Download and extract
        _download_and_extract_zipball(
            release_info["zipball_url"],
            cache_dir,
            show_progress=True,
        )

        # Save metadata
        _save_cache_metadata(release_info["tag_name"], release_info["published_at"])

        return cache_dir

    except NetworkError as e:
        # Network error - try to use cached data if available
        if cache_dir.exists() and list(cache_dir.glob("*.csv")):
            print(f"Warning: {e}")
            print(f"Using cached ground truth data from: {cache_dir}")
            return cache_dir

        # No cache available
        raise RemoteError(
            f"Unable to download ground truth data: {e}\n"
            "No cached data available. Please check your internet connection or "
            "use --ground-truth to specify a local directory."
        )

    except CacheError as e:
        raise RemoteError(f"Failed to prepare ground truth cache: {e}")
