"""Asynchronous API to interact with the AHORN dataset service."""

import contextlib
import gzip
import json
import logging
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

import httpx
from httpx_retries import Retry, RetryTransport

from .utils import get_cache_dir

__all__ = [
    "download_dataset_async",
    "get_dataset_url_async",
    "load_dataset_data_async",
    "load_datasets_data_async",
    "read_dataset_async",
]

DATASET_API_URL = "https://ahorn.rwth-aachen.de/api/datasets.json"

logger = logging.getLogger(__name__)


class AttachmentDict(TypedDict):
    url: str
    size: int


class DatasetDict(TypedDict):
    slug: str
    title: str
    tags: list[str]
    attachments: dict[str, AttachmentDict]


class DatasetsDataDict(TypedDict):
    datasets: dict[str, DatasetDict]
    time: str


async def load_datasets_data_async(
    *, cache_lifetime: int | None = None
) -> dict[str, DatasetDict]:
    """Load dataset data from the Ahorn API."""
    datasets_data_cache = get_cache_dir() / "datasets.json"
    if datasets_data_cache.exists() and cache_lifetime is not None:
        cache_mtime = datetime.fromtimestamp(
            datasets_data_cache.stat().st_mtime, tz=UTC
        )
        age_seconds = (datetime.now(tz=UTC) - cache_mtime).total_seconds()
        if age_seconds < cache_lifetime:
            logger.info(
                "Using cached datasets list (age=%.1fs, lifetime=%ss)",
                age_seconds,
                cache_lifetime,
            )
            with datasets_data_cache.open("r", encoding="utf-8") as cache_file:
                cache: DatasetsDataDict = json.load(cache_file)
                return cache["datasets"]

    logger.info("Fetching datasets list from %s", DATASET_API_URL)
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(DATASET_API_URL)
        response.raise_for_status()

    datasets_data_cache.parent.mkdir(parents=True, exist_ok=True)
    with datasets_data_cache.open("w", encoding="utf-8") as cache_file:
        cache_file.write(response.text)

    response_json: DatasetsDataDict = response.json()
    return response_json["datasets"]


async def load_dataset_data_async(
    slug: str, *, cache_lifetime: int | None = None
) -> DatasetDict:
    """Load data for a specific dataset by its slug."""
    datasets = await load_datasets_data_async(cache_lifetime=cache_lifetime)

    if slug not in datasets:
        raise ValueError(f"Dataset with slug '{slug}' does not exist in AHORN.")

    return datasets[slug]


async def get_dataset_url_async(
    slug: str, revision: int | None = None, *, cache_lifetime: int | None = None
) -> httpx.URL:
    """Get the download URL for a specific dataset by its slug.

    Examples
    --------
    >>> import asyncio
    >>> asyncio.run(get_dataset_url_async("karate-club", revision=2))
    URL('https://zenodo.org/records/19236080/files/karate-club.txt?download=1')
    """
    data = await load_dataset_data_async(slug, cache_lifetime=cache_lifetime)

    revisions = [key for key in data["attachments"] if key.startswith("revision-")]

    if not revisions:
        raise RuntimeError(
            f"Dataset '{slug}' does not contain any revision attachments. "
            "This is an error with the dataset metadata and should be reported to the maintainers."
        )

    if revision is None:
        revision_numbers = [int(key.split("-")[1]) for key in revisions]
        latest_revision = max(revision_numbers)
        revision_key = f"revision-{latest_revision}"
        logger.info(
            "No revision was explicitly specified, using latest revision: %d",
            latest_revision,
        )
    else:
        revision_key = f"revision-{revision}"
        if revision_key not in data["attachments"]:
            available = sorted(int(key.split("-")[1]) for key in revisions)
            raise ValueError(
                f"Dataset '{slug}' does not have revision {revision}. "
                f"Available revisions: {available}"
            )

    return httpx.URL(data["attachments"][revision_key]["url"])


async def download_dataset_async(
    slug: str,
    folder: Path | str,
    revision: int | None = None,
    *,
    cache_lifetime: int | None = None,
) -> Path:
    """Download a dataset by its slug to the specified folder."""
    if isinstance(folder, str):
        folder = Path(folder)

    logger.info("Preparing download for dataset '%s' into %s", slug, folder)
    download_url = await get_dataset_url_async(
        slug,
        revision,
        cache_lifetime=cache_lifetime,
    )

    folder.mkdir(parents=True, exist_ok=True)
    filepath = folder / download_url.path.split("/")[-1]

    retry = Retry(total=5, backoff_factor=2.0)
    retry_transport = RetryTransport(retry=retry)

    async with (
        httpx.AsyncClient(transport=retry_transport, timeout=10) as client,
        client.stream("GET", download_url) as response,
    ):
        response.raise_for_status()

        with filepath.open("wb") as file_handle:
            async for chunk in response.aiter_bytes(chunk_size=8192):
                file_handle.write(chunk)

    logger.info("Downloaded dataset '%s' to %s", slug, filepath)

    return filepath


async def _resolve_dataset_filepath_async(
    slug: str, revision: int | None = None
) -> Path:
    """Resolve the local cached path for a dataset, downloading it if needed."""
    download_url = await get_dataset_url_async(slug, revision)
    filepath = get_cache_dir() / download_url.path.split("/")[-1]

    if not filepath.exists():
        filepath = await download_dataset_async(slug, get_cache_dir(), revision)

    return filepath


@contextlib.contextmanager
def _open_dataset_file(filepath: Path) -> Iterator[Iterator[str]]:
    """Open a dataset file and yield its text lines iterator."""
    if filepath.suffix == ".gz":
        with gzip.open(filepath, mode="rt", encoding="utf-8") as file_handle:
            yield file_handle
    else:
        with filepath.open("r", encoding="utf-8") as file_handle:
            yield file_handle


@contextlib.asynccontextmanager
async def read_dataset_async(
    slug: str, revision: int | None = None
) -> AsyncIterator[Iterator[str]]:
    """Download and yield a context-managed file object for the dataset lines.

    Examples
    --------
    >>> async def _read_first_line() -> str:
    ...     from ahorn_loader.api_async import read_dataset_async
    ...
    ...     async with read_dataset_async("karate-club", revision=2) as dataset:
    ...         return next(dataset).strip()
    >>> import asyncio
    >>> asyncio.run(_read_first_line())
    '{"name": "karate-club", "format-version": "0.3", "revision": 2}'
    """
    filepath = await _resolve_dataset_filepath_async(slug, revision)

    with _open_dataset_file(filepath) as dataset:
        yield dataset
