"""Tests for retry/backoff behavior in download_dataset using httpx+httpx-retries."""

from pathlib import Path

import httpx
import pytest
from httpx import MockTransport
from httpx_retries import Retry, RetryTransport
from pytest import MonkeyPatch

import ahorn_loader.api as api


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
                "revision-1": {
                    "url": "https://example.com/test.txt",
                    "size": 4,
                }
            },
        }

    monkeypatch.setattr(api, "load_dataset_data", _fake_load_dataset_data)


def test_download_dataset_retries_on_429_with_retry_after(
    tmp_path: Path, monkeypatch: MonkeyPatch, fake_dataset: None
) -> None:
    """When a 429 with Retry-After is returned, the next attempt succeeds."""
    call_count = {"value": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["value"] += 1
        if call_count["value"] == 1:
            return httpx.Response(
                status_code=429,
                headers={"Retry-After": "1"},
                request=request,
                content=b"",
            )
        return httpx.Response(status_code=200, request=request, content=b"hello")

    mock_transport = MockTransport(handler)

    def fake_init(self, transport=None, retry=None):  # type: ignore[no-untyped-def]
        self.retry = retry or Retry(total=5, backoff_factor=2.0)
        self._sync_transport = mock_transport
        self._async_transport = None

    monkeypatch.setattr(RetryTransport, "__init__", fake_init)  # type: ignore[arg-type]

    out_path = api.download_dataset("dummy-slug", tmp_path)

    assert call_count["value"] == 2  # initial + one retry
    assert out_path.exists()
    assert out_path.read_bytes() == b"hello"


def test_download_dataset_retries_on_429_without_retry_after(
    tmp_path: Path, monkeypatch: MonkeyPatch, fake_dataset: None
) -> None:
    """When a 429 without Retry-After is returned, we retry and succeed."""
    call_count = {"value": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["value"] += 1
        if call_count["value"] == 1:
            return httpx.Response(status_code=429, request=request, content=b"")
        return httpx.Response(status_code=200, request=request, content=b"world")

    mock_transport = MockTransport(handler)

    def fake_init(self, transport=None, retry=None):  # type: ignore[no-untyped-def]
        self.retry = retry or Retry(total=5, backoff_factor=2.0)
        self._sync_transport = mock_transport
        self._async_transport = None

    monkeypatch.setattr(RetryTransport, "__init__", fake_init)  # type: ignore[arg-type]

    out_path = api.download_dataset("dummy-slug", tmp_path)

    assert call_count["value"] == 2
    assert out_path.exists()
    assert out_path.read_bytes() == b"world"


def test_download_dataset_raises_after_exhausting_429_retries(
    tmp_path: Path, monkeypatch: MonkeyPatch, fake_dataset: None
) -> None:
    """After exhausting retries on repeated 429 responses, an error is raised."""
    call_count = {"value": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["value"] += 1
        return httpx.Response(
            status_code=429,
            headers={"Retry-After": "1"},
            request=request,
            content=b"",
        )

    mock_transport = MockTransport(handler)

    def fake_init(self, transport=None, retry=None):  # type: ignore[no-untyped-def]
        self.retry = retry or Retry(total=5, backoff_factor=0.0)
        self._sync_transport = mock_transport
        self._async_transport = None

    monkeypatch.setattr(RetryTransport, "__init__", fake_init)  # type: ignore[arg-type]

    with pytest.raises(httpx.HTTPStatusError):
        api.download_dataset("dummy-slug", tmp_path)

    # 1 initial attempt + 5 retries
    assert call_count["value"] == 6
