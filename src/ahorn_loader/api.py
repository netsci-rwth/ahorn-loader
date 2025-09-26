"""Module to interact with the Ahorn dataset API."""

import json
import warnings
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
CACHE_PATH = get_cache_dir() / "datasets.json"


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
    if CACHE_PATH.exists() and cache_lifetime is not None:
        cache_mtime = datetime.fromtimestamp(CACHE_PATH.stat().st_mtime, tz=UTC)
        if (datetime.now(tz=UTC) - cache_mtime).total_seconds() < cache_lifetime:
            with CACHE_PATH.open("r", encoding="utf-8") as cache_file:
                cache: DatasetsDataDict = json.load(cache_file)
                return cache["datasets"]

    response = requests.get(DATASET_API_URL, timeout=10)
    response.raise_for_status()

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CACHE_PATH.open("w", encoding="utf-8") as cache_file:
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

    response = requests.get(dataset_attachment["url"], timeout=10, stream=True)
    response.raise_for_status()

    with filepath.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    return filepath


def read_dataset(slug: str) -> str:
    """Download and read a dataset by its slug.

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
    str
        The dataset file content. Be cautious that this string may be really big for
        some datasets. In that case, iterate over the dataset file yourself.

    Raises
    ------
    ValueError
        If the dataset with the given `slug` does not exist.
    RuntimeError
        If the dataset file could not be downloaded due to other errors.
    """
    filepath = download_dataset(slug, get_cache_dir())
    file_size = filepath.stat().st_size
    if file_size > 100 * 1024 * 1024:  # 100 MB threshold
        warnings.warn(
            f"Dataset file is large ({file_size / (1024 * 1024):.1f} MB). "
            "Consider using ahorn_loader.download_dataset() and reading the file in chunks.",
            UserWarning,
            stacklevel=2,
        )
    return filepath.read_text()
