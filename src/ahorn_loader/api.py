"""Module to interact with the Ahorn dataset API."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

__all__ = ["load_datasets_data"]

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
