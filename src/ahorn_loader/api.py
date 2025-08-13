"""Module to interact with the Ahorn dataset API."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

__all__ = ["download_dataset", "load_dataset_data", "load_datasets_data"]

DATASET_API_URL = "https://ahorn.rwth-aachen.de/api/datasets.json"
CACHE_PATH = Path(__file__).parent.parent.parent / "cache" / "datasets.json"


def load_datasets_data(*, cache_lifetime: int | None = None) -> dict[str, Any]:
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
        with CACHE_PATH.open("r", encoding="utf-8") as cache_file:
            cache = json.load(cache_file)
        if (
            cache.get("time")
            and (
                datetime.now(tz=UTC) - datetime.fromisoformat(cache["time"])
            ).total_seconds()
            < cache_lifetime
        ):
            return cache["datasets"]

    response = requests.get(DATASET_API_URL, timeout=10)
    response.raise_for_status()

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CACHE_PATH.open("w", encoding="utf-8") as cache_file:
        cache_file.write(response.text)

    return response.json()["datasets"]


def load_dataset_data(
    slug: str, *, cache_lifetime: int | None = None
) -> dict[str, Any]:
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
    dict[str, Any]
        Dictionary containing the dataset details.
    """
    datasets = load_datasets_data(cache_lifetime=cache_lifetime)
    if "error" in datasets:
        return {"error": datasets["error"]}

    return datasets.get(slug, {"error": f"Dataset '{slug}' not found."})


def download_dataset(
    slug: str, folder: Path | str, *, cache_lifetime: int | None = None
) -> None:
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
    """
    if isinstance(folder, str):
        folder = Path(folder)

    data = load_dataset_data(slug, cache_lifetime=cache_lifetime)
    if "error" in data:
        raise ValueError(f"Error loading dataset '{slug}': {data['error']}")
    if "attachments" not in data or "dataset" not in data["attachments"]:
        raise KeyError(f"Dataset '{slug}' does not contain required 'attachments/dataset' keys.")
    dataset_attachment = data["attachments"]["dataset"]

    response = requests.get(dataset_attachment["url"], timeout=10, stream=True)
    response.raise_for_status()
    folder.mkdir(parents=True, exist_ok=True)
    filepath = folder / dataset_attachment["name"]
    with filepath.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
