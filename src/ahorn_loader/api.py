"""Module to interact with the Ahorn dataset API."""

import contextlib
import gzip
import json
import logging
import time
from collections.abc import Generator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict
from urllib.parse import ParseResult, urlparse

import requests

from .utils import get_cache_dir

__all__ = [
    "download_dataset",
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
        if (datetime.now(tz=UTC) - cache_mtime).total_seconds() < cache_lifetime:
            with datasets_data_cache.open("r", encoding="utf-8") as cache_file:
                cache: DatasetsDataDict = json.load(cache_file)
                return cache["datasets"]

    response = requests.get(DATASET_API_URL, timeout=10)
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

    data = load_dataset_data(slug, cache_lifetime=cache_lifetime)
    if "dataset" not in data["attachments"]:
        raise RuntimeError(
            f"Dataset '{slug}' does not contain required 'attachments/dataset' keys."
        )
    dataset_attachment = data["attachments"]["dataset"]

    url: ParseResult = urlparse(dataset_attachment["url"])
    folder.mkdir(parents=True, exist_ok=True)
    filepath = folder / url.path.split("/")[-1]

    max_retries = 5
    attempt = 0
    while True:
        with requests.get(
            dataset_attachment["url"], timeout=10, stream=True
        ) as response:
            # Exponential backoff if we are rate limited
            if response.status_code == 429:
                attempt += 1
                if attempt > max_retries:
                    raise RuntimeError(
                        f"Rate limited when downloading '{slug}' and max retries exceeded."
                    )
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = (
                        int(retry_after)
                        if retry_after is not None
                        else min(2**attempt, 30)
                    )
                except ValueError:
                    delay = min(2**attempt, 30)
                logger.info(
                    "Rate limited (429) downloading '%s'; sleeping %ss before retry %d/%d",
                    slug,
                    delay,
                    attempt,
                    max_retries,
                )
                time.sleep(delay)
                continue

            # Raise for other HTTP errors
            response.raise_for_status()

            with filepath.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            break

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
    filepath = download_dataset(slug, get_cache_dir())
    if filepath.suffix == ".gz":
        with gzip.open(filepath, mode="rt", encoding="utf-8") as f:
            yield f
    else:
        with filepath.open("r", encoding="utf-8") as f:
            yield f
