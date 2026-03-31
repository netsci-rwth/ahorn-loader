"""Cache-related utility functions.

This module is intentionally split into small platform-specific helper functions plus
one public wrapper. The helpers operate on ``PurePath`` variants so their behaviour can
be tested on any host platform, including Windows path handling on Linux CI.
The public ``get_cache_dir()`` function converts the pure paths back into a concrete
``Path`` object at the boundary where the rest of the package consumes the result.

The cache helpers return ``PurePath`` objects so tests can validate path semantics
without relying on the current operating system. The public API converts these to
concrete ``Path`` objects for use in the rest of the package.
"""

import os
import sys
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

__all__ = ["get_cache_dir"]


def _to_path(path: PurePath) -> Path:
    return Path(str(path))


def _get_windows_cache_dir(
    localappdata: str | None,
    fallback_home: PureWindowsPath,
) -> PureWindowsPath:
    base_path = PureWindowsPath(localappdata or "")
    if not base_path.is_absolute():
        base_path = fallback_home / "AppData" / "Local"
    return base_path / "ahorn-loader" / "Cache"


def _get_macos_cache_dir(home: PurePosixPath) -> PurePosixPath:
    return home / "Library" / "Caches" / "ahorn-loader"


def _get_unix_cache_dir(
    xdg_cache_home: str | None,
    home: PurePosixPath,
) -> PurePosixPath:
    base_path = PurePosixPath(xdg_cache_home or "")
    if not base_path.is_absolute():
        base_path = home / ".cache"
    return base_path / "ahorn-loader"


def get_cache_dir() -> Path:
    """Return an appropriate cache location for the current platform.

    The platform-specific logic lives in pure helper functions so it stays easy to
    test in isolation. This wrapper is responsible for reading environment variables,
    selecting the correct helper for ``sys.platform``, and returning a concrete
    ``pathlib.Path`` for callers.

    Returns
    -------
    pathlib.Path
        Platform-dependent cache directory.
    """
    user_home = Path.home()

    match sys.platform:
        case "win32":
            return _to_path(
                _get_windows_cache_dir(
                    os.getenv("LOCALAPPDATA"),
                    PureWindowsPath(user_home),
                )
            )
        case "darwin":
            return _to_path(_get_macos_cache_dir(PurePosixPath(user_home)))
        case _:
            return _to_path(
                _get_unix_cache_dir(
                    os.getenv("XDG_CACHE_HOME"),
                    PurePosixPath(user_home),
                )
            )
