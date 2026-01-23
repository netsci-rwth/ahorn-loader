"""Module to interact with the Ahorn dataset API."""

import contextlib
import gzip
import json
import logging
from collections.abc import Generator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

import httpx
from httpx_retries import Retry, RetryTransport

from .utils import get_cache_dir

__all__ = [
    "download_dataset",
    "get_dataset_url",
    "load_dataset_data",
    "load_datasets_data",
    "read_dataset",
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


def load_datasets_data(*, cache_lifetime: int | None = None) -> dict[str, DatasetDict]:
    """Load dataset data from the Ahorn API.

    Parameters
    ----------
    cache_lifetime : int, optional
        How long to reuse cached data in seconds. If not provided, the cache will not
        be used.

    Returns
    -------
    dict[str, Any]
        Dictionary containing dataset information, where the keys are dataset slugs
        and the values are dictionaries with dataset details such as title, tags, and
        attachments.
    """
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
    response = httpx.get(DATASET_API_URL, timeout=10)
    response.raise_for_status()

    datasets_data_cache.parent.mkdir(parents=True, exist_ok=True)
    with datasets_data_cache.open("w", encoding="utf-8") as cache_file:
        cache_file.write(response.text)

    response_json: DatasetsDataDict = response.json()
    return response_json["datasets"]


def load_dataset_data(slug: str, *, cache_lifetime: int | None = None) -> DatasetDict:
    """Load data for a specific dataset by its slug.

    Parameters
    ----------
    slug : str
        The slug of the dataset to load.
    cache_lifetime : int, optional
        How long to reuse cached data in seconds. If not provided, the cache will not
        be used.

    Returns
    -------
    DatasetDict
        Dictionary containing the dataset details.

    Raises
    ------
    KeyError
        If the dataset with the given `slug` does not exist.
    """
    datasets = load_datasets_data(cache_lifetime=cache_lifetime)

    if slug not in datasets:
        raise KeyError(f"Dataset with slug '{slug}' does not exist in AHORN.")

    return datasets[slug]


def get_dataset_url(slug: str, *, cache_lifetime: int | None = None) -> httpx.URL:
    """Get the download URL for a specific dataset by its slug.

    Parameters
    ----------
    slug : str
        The slug of the dataset.
    cache_lifetime : int, optional
        How long to reuse cached data in seconds. If not provided, the cache will not
        be used.

    Returns
    -------
    httpx.URL
        The download URL of the dataset.

    Raises
    ------
    KeyError
        If the dataset with the given `slug` does not exist.
    RuntimeError
        If the dataset does not contain the required attachment information.
    """
    data = load_dataset_data(slug, cache_lifetime=cache_lifetime)
    if "dataset" not in data["attachments"]:
        raise RuntimeError(
            f"Dataset '{slug}' does not contain required 'attachments/dataset' keys."
        )
    return httpx.URL(data["attachments"]["dataset"]["url"])


def download_dataset(
    slug: str, folder: Path | str, *, cache_lifetime: int | None = None
) -> Path:
    """Download a dataset by its slug to the specified folder.

    This function implements an exponential backoff strategy when encountering HTTP 429
    (Too Many Requests) responses. If available, it respects the 'Retry-After' header to
    determine the wait time before retrying.

    Parameters
    ----------
    slug : str
        The slug of the dataset to download.
    folder : Path | str
        The folder where the dataset should be saved.
    cache_lifetime : int, optional
        How long to reuse cached data in seconds. If not provided, the cache will not
        be used.

    Returns
    -------
    Path
        The path to the downloaded dataset file.

    Raises
    ------
    KeyError
        If the dataset with the given `slug` does not exist.
    HTTPError
        If the dataset file could not be downloaded due to some error.
    RuntimeError
        If the dataset file could not be downloaded due to some error.
    """
    if isinstance(folder, str):
        folder = Path(folder)

    logger.info("Preparing download for dataset '%s' into %s", slug, folder)
    download_url = get_dataset_url(slug, cache_lifetime=cache_lifetime)

    folder.mkdir(parents=True, exist_ok=True)
    filepath = folder / download_url.path.split("/")[-1]

    # Use RetryTransport to automatically handle rate limiting (429) with exponential
    # backoff. This also automatically respects 'Retry-After' headers if provided.
    retry = Retry(
        total=5,
        backoff_factor=2.0,
    )
    retry_transport = RetryTransport(retry=retry)

    with (
        httpx.Client(transport=retry_transport, timeout=10) as client,
        client.stream("GET", download_url) as response,
    ):
        response.raise_for_status()

        with filepath.open("wb") as f:
            for chunk in response.iter_bytes(chunk_size=8192):
                f.write(chunk)

    logger.info("Downloaded dataset '%s' to %s", slug, filepath)

    return filepath


@contextlib.contextmanager
def read_dataset(slug: str) -> Generator[Iterator[str], None, None]:
    """Download and yield a context-managed file object for the dataset lines by slug.

    The dataset file will be stored in your system cache and can be deleted according
    to your system's cache policy. To ensure that costly re-downloads do not occur, use
    the `download_dataset` function to store the dataset file at a more permanent
    location.

    Parameters
    ----------
    slug : str
        The slug of the dataset to download.

    Returns
    -------
    Context manager yielding an open file object (iterator over lines).

    Raises
    ------
    KeyError
        If the dataset with the given `slug` does not exist.
    RuntimeError
        If the dataset file could not be downloaded due to other errors.

    Examples
    --------
    >>> import ahorn_loader
    >>> with ahorn_loader.read_dataset("contact-high-school") as dataset:
    >>>     for line in dataset:
    >>>         ...
    """
    download_url = get_dataset_url(slug)
    filepath = get_cache_dir() / download_url.path.split("/")[-1]

    # Download the dataset if it is not already cached
    if not filepath.exists():
        filepath = download_dataset(slug, get_cache_dir())

    if filepath.suffix == ".gz":
        with gzip.open(filepath, mode="rt", encoding="utf-8") as f:
            yield f
    else:
        with filepath.open("r", encoding="utf-8") as f:
            yield f
