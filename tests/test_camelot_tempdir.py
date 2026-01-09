"""Tests for CamelotTempDir context manager."""

import atexit
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from idn_area_etl.utils import CamelotTempDir


class TestCamelotTempDir:
    """Test the CamelotTempDir context manager."""

    def test_creates_temp_directory_with_default_location(self):
        """Test that CamelotTempDir creates a temp directory in system default location."""
        with CamelotTempDir() as temp_dir:
            # Verify temp directory was created
            assert Path(temp_dir).exists()
            assert Path(temp_dir).is_dir()
            # Verify it has the correct prefix
            assert Path(temp_dir).name.startswith("camelot_")

        # Verify temp directory was cleaned up after exit
        assert not Path(temp_dir).exists()

    def test_creates_temp_directory_with_custom_base_dir(self, tmp_path: Path):
        """Test that CamelotTempDir creates temp directory in custom location."""
        custom_base = tmp_path / "custom_tmp"

        with CamelotTempDir(base_dir=custom_base) as temp_dir:
            # Verify temp directory was created in custom location
            assert Path(temp_dir).exists()
            assert Path(temp_dir).is_dir()
            assert Path(temp_dir).parent == custom_base
            assert Path(temp_dir).name.startswith("camelot_")

        # Verify temp directory was cleaned up
        assert not Path(temp_dir).exists()

    def test_creates_base_dir_if_not_exists(self, tmp_path: Path):
        """Test that CamelotTempDir creates base_dir if it doesn't exist."""
        custom_base = tmp_path / "nested" / "custom" / "tmp"

        # Verify base_dir doesn't exist yet
        assert not custom_base.exists()

        with CamelotTempDir(base_dir=custom_base) as temp_dir:
            # Verify base_dir and temp_dir were created
            assert custom_base.exists()
            assert Path(temp_dir).exists()
            assert Path(temp_dir).parent == custom_base

        # Verify temp directory was cleaned up
        assert not Path(temp_dir).exists()

    def test_uses_custom_prefix(self, tmp_path: Path):
        """Test that CamelotTempDir uses custom prefix."""
        custom_base = tmp_path / "tmp"

        with CamelotTempDir(base_dir=custom_base, prefix="test_prefix_") as temp_dir:
            assert Path(temp_dir).name.startswith("test_prefix_")

        assert not Path(temp_dir).exists()

    def test_overrides_tempfile_tempdir(self, tmp_path: Path):
        """Test that CamelotTempDir overrides tempfile.tempdir."""
        original_tempdir = tempfile.tempdir
        custom_base = tmp_path / "tmp"

        with CamelotTempDir(base_dir=custom_base) as temp_dir:
            # Inside context, tempfile.tempdir should be set to our temp_dir
            assert tempfile.tempdir == temp_dir

        # After exit, tempfile.tempdir should be restored
        assert tempfile.tempdir == original_tempdir

    def test_restores_tempfile_tempdir_on_exception(self, tmp_path: Path):
        """Test that CamelotTempDir restores tempfile.tempdir even on exception."""
        original_tempdir = tempfile.tempdir
        custom_base = tmp_path / "tmp"

        with pytest.raises(ValueError, match="Test exception"):
            with CamelotTempDir(base_dir=custom_base):
                raise ValueError("Test exception")

        # tempfile.tempdir should still be restored
        assert tempfile.tempdir == original_tempdir

    def test_cleans_up_temp_directory_on_exception(self, tmp_path: Path):
        """Test that CamelotTempDir cleans up temp directory even on exception."""
        custom_base = tmp_path / "tmp"
        temp_dir_path = None

        with pytest.raises(ValueError, match="Test exception"):
            with CamelotTempDir(base_dir=custom_base) as temp_dir:
                temp_dir_path = temp_dir
                # Create some files
                (Path(temp_dir) / "test.txt").write_text("test")
                raise ValueError("Test exception")

        # Verify temp directory was cleaned up
        assert temp_dir_path is not None
        assert not Path(temp_dir_path).exists()

    def test_tracks_and_unregisters_atexit_handlers(self, tmp_path: Path):
        """Test that CamelotTempDir tracks and unregisters atexit handlers."""
        custom_base = tmp_path / "tmp"
        tracked_temp_dirs = []

        # Monkey-patch atexit.register to track what was registered
        original_register = atexit.register

        def tracking_register(
            func: Callable[..., Any], *args: Any, **kwargs: Any
        ) -> Callable[..., Any]:
            if func == shutil.rmtree and args:
                tracked_temp_dirs.append(str(args[0]))
            return original_register(func, *args, **kwargs)

        atexit.register = tracking_register  # type: ignore[assignment]

        try:
            with CamelotTempDir(base_dir=custom_base) as temp_dir:
                # Simulate what camelot does: register cleanup handler
                atexit.register(shutil.rmtree, temp_dir)

                # Verify our temp_dir was tracked
                assert str(temp_dir) in tracked_temp_dirs

            # After exit, the tracked temp_dir should have been cleaned up
            # and should not exist
            assert not Path(temp_dir).exists()

        finally:
            # Restore original atexit.register
            atexit.register = original_register  # type: ignore[assignment]

    def test_handles_multiple_atexit_registrations(self, tmp_path: Path):
        """Test that CamelotTempDir handles multiple atexit registrations correctly."""
        custom_base = tmp_path / "tmp"

        with CamelotTempDir(base_dir=custom_base) as temp_dir:
            # Register multiple cleanup handlers (simulating multiple pages)
            subdir1 = Path(temp_dir) / "subdir1"
            subdir1.mkdir()
            atexit.register(shutil.rmtree, str(subdir1))

            subdir2 = Path(temp_dir) / "subdir2"
            subdir2.mkdir()
            atexit.register(shutil.rmtree, str(subdir2))

        # After exit, the parent temp_dir and subdirs should all be cleaned up
        assert not Path(temp_dir).exists()
        assert not subdir1.exists()
        assert not subdir2.exists()

    def test_does_not_interfere_with_unrelated_atexit_handlers(self, tmp_path: Path):
        """Test that CamelotTempDir doesn't unregister unrelated atexit handlers."""
        custom_base = tmp_path / "tmp"

        # Create a file that will be cleaned by our handler
        test_file = tmp_path / "test_cleanup.txt"
        test_file.write_text("test")

        def dummy_handler():
            # This handler should NOT be unregistered
            if test_file.exists():
                test_file.unlink()

        # Register unrelated handler
        atexit.register(dummy_handler)

        with CamelotTempDir(base_dir=custom_base):
            # Context should not affect unrelated handlers
            pass

        # Cleanup: manually call and unregister our dummy handler
        # (in real scenario, it would be called at process exit)
        if test_file.exists():
            dummy_handler()
        atexit.unregister(dummy_handler)

    def test_handles_already_cleaned_directory_gracefully(self, tmp_path: Path):
        """Test that CamelotTempDir handles race conditions gracefully."""
        custom_base = tmp_path / "tmp"

        with CamelotTempDir(base_dir=custom_base) as temp_dir:
            # Manually delete the directory before exit (simulating race condition)
            shutil.rmtree(temp_dir)

        # Should not raise exception even though directory was already deleted
        assert not Path(temp_dir).exists()

    def test_nested_context_managers(self, tmp_path: Path):
        """Test that nested CamelotTempDir context managers work correctly."""
        custom_base = tmp_path / "tmp"

        with CamelotTempDir(base_dir=custom_base, prefix="outer_") as temp_dir1:
            assert Path(temp_dir1).exists()

            with CamelotTempDir(base_dir=custom_base, prefix="inner_") as temp_dir2:
                assert Path(temp_dir2).exists()
                assert temp_dir1 != temp_dir2

            # Inner temp_dir should be cleaned up
            assert not Path(temp_dir2).exists()
            # Outer temp_dir should still exist
            assert Path(temp_dir1).exists()

        # Both should be cleaned up
        assert not Path(temp_dir1).exists()
        assert not Path(temp_dir2).exists()
