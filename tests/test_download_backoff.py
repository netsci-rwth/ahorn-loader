"""Tests for 429 retry/backoff behavior in download_dataset."""

from collections.abc import Iterable
from pathlib import Path

import pytest
from pytest import MonkeyPatch

import ahorn_loader.api as api


class FakeResponse:
    """Minimal Response-like object for simulating requests.get."""

    def __init__(
        self,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        content: bytes = b"data",
    ) -> None:
        self.status_code: int = status_code
        self.headers: dict[str, str] = headers or {}
        self._content: bytes = content

    def __enter__(self) -> "FakeResponse":
        """Enter context manager and return self."""
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        """Exit context manager; no resource to release here."""
        return False

    def raise_for_status(self) -> None:
        """Raise for HTTP error codes except 429 (handled by caller)."""
        if 400 <= self.status_code < 600 and self.status_code != 429:
            raise Exception(f"HTTP error {self.status_code}")

    def iter_content(self, chunk_size: int = 8192) -> Iterable[bytes]:
        """Yield content in a single chunk for simplicity."""
        yield self._content


@pytest.fixture()
def fake_dataset(monkeypatch: MonkeyPatch) -> None:
    """Patch load_dataset_data to avoid real network calls."""

    def _fake_load_dataset_data(
        slug: str, *, cache_lifetime: int | None = None
    ) -> api.DatasetDict:
        return {
            "slug": slug,
            "title": "Test Dataset",
            "tags": [],
            "attachments": {
                "dataset": {
                    "url": "https://example.com/test.txt",
                    "size": 4,
                }
            },
        }

    monkeypatch.setattr(api, "load_dataset_data", _fake_load_dataset_data)


def test_download_dataset_retries_on_429_with_retry_after(
    tmp_path: Path, monkeypatch: MonkeyPatch, fake_dataset: None
) -> None:
    """When 429 includes Retry-After, sleep that many seconds then retry once."""
    calls = {"count": 0}

    responses: list[FakeResponse] = [
        FakeResponse(status_code=429, headers={"Retry-After": "7"}),
        FakeResponse(status_code=200, content=b"hello"),
    ]

    def fake_get(url: str, timeout: int = 10, stream: bool = True) -> FakeResponse:
        calls["count"] += 1
        return responses.pop(0)

    slept: list[int] = []

    def fake_sleep(seconds: int) -> None:
        slept.append(seconds)

    monkeypatch.setattr(api.requests, "get", fake_get)
    monkeypatch.setattr(api.time, "sleep", fake_sleep)

    out_path = api.download_dataset("dummy-slug", tmp_path)

    # Assert retry happened exactly once and we slept the Retry-After seconds
    assert calls["count"] == 2
    assert slept == [7]
    assert out_path.exists()
    assert out_path.read_bytes() == b"hello"


def test_download_dataset_retries_on_429_without_retry_after(
    tmp_path: Path, monkeypatch: MonkeyPatch, fake_dataset: None
) -> None:
    """Without Retry-After, use exponential backoff (first attempt -> 2 seconds)."""
    calls = {"count": 0}

    responses: list[FakeResponse] = [
        FakeResponse(status_code=429, headers={}),  # no Retry-After
        FakeResponse(status_code=200, content=b"world"),
    ]

    def fake_get(url: str, timeout: int = 10, stream: bool = True) -> FakeResponse:
        calls["count"] += 1
        return responses.pop(0)

    slept: list[int] = []

    def fake_sleep(seconds: int) -> None:
        slept.append(seconds)

    monkeypatch.setattr(api.requests, "get", fake_get)
    monkeypatch.setattr(api.time, "sleep", fake_sleep)

    out_path = api.download_dataset("dummy-slug", tmp_path)

    # First retry attempt uses 2**attempt = 2**1 = 2 seconds (capped at 30)
    assert calls["count"] == 2
    assert slept == [2]
    assert out_path.exists()
    assert out_path.read_bytes() == b"world"


def test_download_dataset_raises_after_exhausting_429_retries(
    tmp_path: Path, monkeypatch: MonkeyPatch, fake_dataset: None
) -> None:
    """Raise RuntimeError after max retries when every request returns 429."""
    calls = {"count": 0}

    def fake_get(url: str, timeout: int = 10, stream: bool = True) -> FakeResponse:
        calls["count"] += 1
        return FakeResponse(status_code=429, headers={})

    slept: list[int] = []

    def fake_sleep(seconds: int) -> None:
        slept.append(seconds)

    monkeypatch.setattr(api.requests, "get", fake_get)
    monkeypatch.setattr(api.time, "sleep", fake_sleep)

    with pytest.raises(RuntimeError, match="Rate limited"):
        api.download_dataset("dummy-slug", tmp_path)

    # We attempt once plus five retries -> six calls; five sleeps before failure
    assert calls["count"] == 6
    assert slept == [2, 4, 8, 16, 30]
