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
            base = os.getenv("LOCALAPPDATA") or Path("~\\AppData\\Local").expanduser()
            return Path(base) / "ahorn-loader" / "Cache"
        case "darwin":
            return Path.home() / "Library" / "Caches" / "ahorn-loader"
        case _:
            # Linux and other Unix
            xdg = os.getenv("XDG_CACHE_HOME")
            if xdg:
                return Path(xdg) / "ahorn-loader"
            return Path.home() / ".cache" / "ahorn-loader"
