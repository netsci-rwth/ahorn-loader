"""Synchronous API to interact with AHORN datasets."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from .api_async import (
    DatasetDict,
    _open_dataset_file,
    _resolve_dataset_filepath_async,
    download_dataset_async,
    get_dataset_url_async,
    load_dataset_data_async,
    load_datasets_data_async,
)
from .validator import Validator

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Iterator
    from pathlib import Path
    from typing import Any

    import httpx

__all__ = [
    "download_dataset",
    "get_dataset_url",
    "load_dataset_data",
    "load_datasets_data",
    "read_dataset",
    "validate_dataset",
]


def _run_sync[T](
    awaitable_factory: Callable[[], Coroutine[Any, Any, T]], *, api_name: str
) -> T:
    """Run an async API call from synchronous code."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable_factory())

    raise RuntimeError(
        f"{api_name}() cannot be used while an event loop is already running. "
        "Use the async API from ahorn_loader.api_async instead."
    )


def load_datasets_data(*, cache_lifetime: int | None = None) -> dict[str, DatasetDict]:
    """Load dataset data from the AHORN API synchronously.

    Examples
    --------
    >>> "karate-club" in ahorn_loader.load_datasets_data(cache_lifetime=3600)
    True
    """
    return _run_sync(
        lambda: load_datasets_data_async(cache_lifetime=cache_lifetime),
        api_name="load_datasets_data",
    )


def load_dataset_data(slug: str, *, cache_lifetime: int | None = None) -> DatasetDict:
    """Load data for a specific dataset synchronously.

    Examples
    --------
    >>> ahorn_loader.load_dataset_data("karate-club", cache_lifetime=3600)["slug"]
    'karate-club'
    """
    return _run_sync(
        lambda: load_dataset_data_async(slug, cache_lifetime=cache_lifetime),
        api_name="load_dataset_data",
    )


def get_dataset_url(
    slug: str, revision: int | None = None, *, cache_lifetime: int | None = None
) -> httpx.URL:
    """Get the download URL for a specific dataset synchronously.

    Examples
    --------
    >>> ahorn_loader.get_dataset_url("karate-club", revision=2, cache_lifetime=3600)
    URL('https://zenodo.org/records/19236080/files/karate-club.txt?download=1')
    """
    return _run_sync(
        lambda: get_dataset_url_async(
            slug,
            revision,
            cache_lifetime=cache_lifetime,
        ),
        api_name="get_dataset_url",
    )


def download_dataset(
    slug: str,
    folder: Path | str,
    revision: int | None = None,
    *,
    cache_lifetime: int | None = None,
) -> Path:
    """Download a dataset by its slug to the specified folder synchronously.

    Examples
    --------
    >>> from tempfile import TemporaryDirectory
    >>> with TemporaryDirectory() as tmp_dir:
    ...     ahorn_loader.download_dataset("karate-club", tmp_dir, revision=2).name
    'karate-club.txt'
    """
    return _run_sync(
        lambda: download_dataset_async(
            slug,
            folder,
            revision,
            cache_lifetime=cache_lifetime,
        ),
        api_name="download_dataset",
    )


@contextlib.contextmanager
def read_dataset(slug: str, revision: int | None = None) -> Iterator[Iterator[str]]:
    """Download and yield a file object for a dataset from synchronous code.

    Examples
    --------
    >>> with ahorn_loader.read_dataset("karate-club", revision=2) as dataset:
    ...     next(dataset).strip()
    '{"name": "karate-club", "format-version": "0.3", "revision": 2}'
    """
    filepath = _run_sync(
        lambda: _resolve_dataset_filepath_async(slug, revision),
        api_name="read_dataset",
    )

    with _open_dataset_file(filepath) as dataset:
        yield dataset


def validate_dataset(path: str | Path) -> bool:
    r"""Validate whether a given file is a valid AHORN dataset.

    Validation errors are logged, but not raised as exceptions.

    Examples
    --------
    >>> from pathlib import Path
    >>> from tempfile import TemporaryDirectory
    >>> from ahorn_loader import validate_dataset
    >>> with TemporaryDirectory() as tmp_dir:
    ...     dataset_path = Path(tmp_dir) / "demo.txt"
    ...     _ = dataset_path.write_text(
    ...         '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
    ...         '1 {"weight": 1.0}\n'
    ...         '1,2 {"weight": 2.0}\n'
    ...     )
    ...     validate_dataset(dataset_path)
    True
    """
    validator = Validator()
    return validator.validate(path)
