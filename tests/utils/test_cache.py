"""Tests for cache utility functions."""

from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from ahorn_loader.utils.cache import (
    _get_macos_cache_dir,
    _get_unix_cache_dir,
    _get_windows_cache_dir,
    get_cache_dir,
)

WINDOWS_HOME = PureWindowsPath("C:\\Users\\test")
WINDOWS_LOCALAPPDATA = PureWindowsPath("C:\\Users\\test\\AppData\\Local")
POSIX_HOME = PurePosixPath("/home/test")


@pytest.mark.parametrize(
    ("localappdata", "fallback_home", "expected"),
    (
        (
            "C:\\Users\\test\\AppData\\Local",
            PureWindowsPath("C:\\Users\\fallback"),
            WINDOWS_LOCALAPPDATA / "ahorn-loader" / "Cache",
        ),
        (
            None,
            WINDOWS_HOME,
            WINDOWS_LOCALAPPDATA / "ahorn-loader" / "Cache",
        ),
        (
            "relative\\path",
            WINDOWS_HOME,
            WINDOWS_LOCALAPPDATA / "ahorn-loader" / "Cache",
        ),
    ),
)
def test_windows_cache_dir_helper(
    localappdata: str | None,
    fallback_home: PureWindowsPath,
    expected: PureWindowsPath,
) -> None:
    """Windows cache helper should handle absolute, missing, and relative inputs."""
    assert _get_windows_cache_dir(localappdata, fallback_home) == expected


def test_macos_cache_dir_helper() -> None:
    """The macOS helper should use the Library/Caches convention."""
    assert _get_macos_cache_dir(PurePosixPath("/Users/test")) == PurePosixPath(
        "/Users/test/Library/Caches/ahorn-loader"
    )


@pytest.mark.parametrize(
    ("xdg_cache_home", "home", "expected"),
    (
        (
            "/home/test/.cache",
            PurePosixPath("/home/fallback"),
            PurePosixPath("/home/test/.cache/ahorn-loader"),
        ),
        (
            ".cache",
            POSIX_HOME,
            PurePosixPath("/home/test/.cache/ahorn-loader"),
        ),
        (
            None,
            POSIX_HOME,
            PurePosixPath("/home/test/.cache/ahorn-loader"),
        ),
    ),
)
def test_unix_cache_dir_helper(
    xdg_cache_home: str | None,
    home: PurePosixPath,
    expected: PurePosixPath,
) -> None:
    """Unix cache helper should handle XDG overrides and fallbacks."""
    assert _get_unix_cache_dir(xdg_cache_home, home) == expected


@pytest.mark.parametrize(
    ("platform_name", "env_var", "env_value", "home", "expected"),
    (
        (
            "win32",
            "LOCALAPPDATA",
            "C:\\Users\\test\\AppData\\Local",
            Path("C:/Users/test"),
            Path("C:\\Users\\test\\AppData\\Local\\ahorn-loader\\Cache"),
        ),
        (
            "darwin",
            None,
            None,
            Path("/Users/test"),
            Path("/Users/test/Library/Caches/ahorn-loader"),
        ),
        (
            "linux",
            "XDG_CACHE_HOME",
            "/home/test/.cache",
            Path("/home/ignored"),
            Path("/home/test/.cache/ahorn-loader"),
        ),
        (
            "linux",
            "XDG_CACHE_HOME",
            None,
            Path("/home/test"),
            Path("/home/test/.cache/ahorn-loader"),
        ),
        (
            "freebsd",
            "XDG_CACHE_HOME",
            None,
            Path("/home/test"),
            Path("/home/test/.cache/ahorn-loader"),
        ),
    ),
)
def test_get_cache_dir(
    monkeypatch: pytest.MonkeyPatch,
    platform_name: str,
    env_var: str | None,
    env_value: str | None,
    home: Path,
    expected: Path,
) -> None:
    """The public wrapper should dispatch correctly for each supported platform."""
    monkeypatch.setattr("sys.platform", platform_name)
    monkeypatch.setattr(Path, "home", lambda: home)

    if env_var is not None and env_value is not None:
        monkeypatch.setenv(env_var, env_value)
    elif env_var is not None:
        monkeypatch.delenv(env_var, raising=False)

    assert get_cache_dir() == expected


def test_get_cache_dir_returns_path_object() -> None:
    """The public API should always return a concrete Path object."""
    assert isinstance(get_cache_dir(), Path)
