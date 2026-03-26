"""Module with cache-related utility functions."""

import os
import sys
from pathlib import Path

__all__ = ["get_cache_dir"]


def get_cache_dir() -> Path:
    """Return an appropriate cache location for the current platform.

    Returns
    -------
    pathlib.Path
        Platform-dependent cache directory.
    """
    match sys.platform:
        case "win32":
            # Prefer LOCALAPPDATA if it points to an absolute path; otherwise fall back
            # to the standard location under the user's home directory.
            base_path = Path(os.getenv("LOCALAPPDATA", ""))
            if not base_path.is_absolute():
                base_path = Path("~\\AppData\\Local").expanduser()
            return base_path / "ahorn-loader" / "Cache"
        case "darwin":
            return Path.home() / "Library" / "Caches" / "ahorn-loader"
        case _:
            # Linux and other Unix
            base_path = Path(os.getenv("XDG_CACHE_HOME", ""))
            if not base_path.is_absolute():
                base_path = Path.home() / ".cache"
            return base_path / "ahorn-loader"
