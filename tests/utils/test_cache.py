"""Test the cache utility functions."""

import os
from pathlib import Path
from unittest.mock import patch

from ahorn_loader.utils.cache import get_cache_dir


class TestGetCacheDir:
    """Tests for get_cache_dir function."""

    @patch("sys.platform", "win32")
    @patch.dict(os.environ, {"LOCALAPPDATA": "C:\\Users\\test\\AppData\\Local"})
    def test_windows_with_localappdata(self):
        """Test Windows cache directory with LOCALAPPDATA set."""
        result = get_cache_dir()
        expected = Path("C:\\Users\\test\\AppData\\Local") / "ahorn-loader" / "Cache"
        assert result == expected

    @patch("sys.platform", "win32")
    @patch.dict(os.environ, {}, clear=True)
    @patch("pathlib.Path.expanduser")
    def test_windows_without_localappdata(self, mock_expanduser):
        """Test Windows cache directory without LOCALAPPDATA."""
        mock_expanduser.return_value = Path("C:\\Users\\test\\AppData\\Local")
        result = get_cache_dir()
        expected = Path("C:\\Users\\test\\AppData\\Local") / "ahorn-loader" / "Cache"
        assert result == expected

    @patch("sys.platform", "darwin")
    @patch("pathlib.Path.home")
    def test_macos(self, mock_home):
        """Test macOS cache directory."""
        mock_home.return_value = Path("/Users/test")
        result = get_cache_dir()
        expected = Path("/Users/test") / "Library" / "Caches" / "ahorn-loader"
        assert result == expected

    @patch("sys.platform", "linux")
    @patch.dict(os.environ, {"XDG_CACHE_HOME": "/home/test/.cache"})
    def test_linux_with_xdg_cache_home(self):
        """Test Linux cache directory with XDG_CACHE_HOME set."""
        result = get_cache_dir()
        expected = Path("/home/test/.cache") / "ahorn-loader"
        assert result == expected

    @patch("sys.platform", "linux")
    @patch.dict(os.environ, {}, clear=True)
    @patch("pathlib.Path.home")
    def test_linux_without_xdg_cache_home(self, mock_home):
        """Test Linux cache directory without XDG_CACHE_HOME."""
        mock_home.return_value = Path("/home/test")
        result = get_cache_dir()
        expected = Path("/home/test") / ".cache" / "ahorn-loader"
        assert result == expected

    @patch("sys.platform", "freebsd")
    @patch.dict(os.environ, {}, clear=True)
    @patch("pathlib.Path.home")
    def test_other_unix_platforms(self, mock_home):
        """Test other Unix platforms cache directory."""
        mock_home.return_value = Path("/home/test")
        result = get_cache_dir()
        expected = Path("/home/test") / ".cache" / "ahorn-loader"
        assert result == expected

    def test_returns_path_object(self):
        """Test that get_cache_dir returns a Path object."""
        result = get_cache_dir()
        assert isinstance(result, Path)
