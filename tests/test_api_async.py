"""Tests for the asynchronous AHORN API surface."""

import asyncio
import gzip
from pathlib import Path

import httpx
import pytest
from pytest import MonkeyPatch

import ahorn_loader.api_async as api_async


def test_get_dataset_url_raises_when_dataset_has_no_revisions(
    monkeypatch: MonkeyPatch,
) -> None:
    """A dataset without revision attachments should raise a clear RuntimeError."""

    async def fake_load_dataset_data(
        slug: str, *, cache_lifetime: int | None = None
    ) -> api_async.DatasetDict:
        return {
            "slug": slug,
            "title": "Broken Dataset",
            "tags": [],
            "attachments": {
                "thumbnail": {
                    "url": "https://example.com/thumb.png",
                    "size": 42,
                }
            },
        }

    monkeypatch.setattr(
        api_async,
        "load_dataset_data_async",
        fake_load_dataset_data,
    )

    with pytest.raises(
        RuntimeError,
        match="does not contain any revision attachments",
    ):
        asyncio.run(api_async.get_dataset_url_async("broken-dataset"))


def test_get_dataset_url_raises_for_unknown_revision(
    monkeypatch: MonkeyPatch,
) -> None:
    """An unavailable revision should raise a ValueError listing alternatives."""

    async def fake_load_dataset_data(
        slug: str, *, cache_lifetime: int | None = None
    ) -> api_async.DatasetDict:
        return {
            "slug": slug,
            "title": "Versioned Dataset",
            "tags": [],
            "attachments": {
                "revision-1": {
                    "url": "https://example.com/revision-1.txt",
                    "size": 10,
                },
                "revision-3": {
                    "url": "https://example.com/revision-3.txt",
                    "size": 30,
                },
            },
        }

    monkeypatch.setattr(
        api_async,
        "load_dataset_data_async",
        fake_load_dataset_data,
    )

    with pytest.raises(
        ValueError,
        match=r"does not have revision 2\. Available revisions: \[1, 3\]",
    ):
        asyncio.run(api_async.get_dataset_url_async("versioned-dataset", revision=2))


def test_read_dataset_async_reads_plain_text_dataset(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """The async reader should yield lines from a plain-text dataset file."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 2, "format-version": "0.1"}\n1 {"weight": 1.0}\n',
        encoding="utf-8",
    )

    async def fake_resolve_dataset_filepath(
        slug: str, revision: int | None = None
    ) -> Path:
        assert slug == "demo"
        assert revision == 2
        return dataset_path

    monkeypatch.setattr(
        api_async,
        "_resolve_dataset_filepath_async",
        fake_resolve_dataset_filepath,
    )

    async def read_first_line() -> str:
        async with api_async.read_dataset_async("demo", revision=2) as dataset:
            return next(dataset).strip()

    assert asyncio.run(read_first_line()) == (
        '{"name": "demo", "revision": 2, "format-version": "0.1"}'
    )


def test_read_dataset_async_reads_gzip_dataset(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """The async reader should transparently open gzip-compressed dataset files."""
    dataset_path = tmp_path / "demo.txt.gz"
    with gzip.open(dataset_path, mode="wt", encoding="utf-8") as file_handle:
        file_handle.write(
            '{"name": "demo", "revision": 3, "format-version": "0.1"}\n'
            '1 {"weight": 1.0}\n'
        )

    async def fake_resolve_dataset_filepath(
        slug: str, revision: int | None = None
    ) -> Path:
        assert slug == "demo"
        assert revision == 3
        return dataset_path

    monkeypatch.setattr(
        api_async,
        "_resolve_dataset_filepath_async",
        fake_resolve_dataset_filepath,
    )

    async def read_first_line() -> str:
        async with api_async.read_dataset_async("demo", revision=3) as dataset:
            return next(dataset).strip()

    assert asyncio.run(read_first_line()) == (
        '{"name": "demo", "revision": 3, "format-version": "0.1"}'
    )


def test_resolve_dataset_filepath_async_downloads_on_cache_miss(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Missing cached files should trigger a download and return the new path."""
    downloaded_path = tmp_path / "downloaded.txt"

    async def fake_get_dataset_url(slug: str, revision: int | None = None) -> httpx.URL:
        assert slug == "demo"
        assert revision == 4
        return httpx.URL("https://example.com/cache-miss.txt")

    async def fake_download_dataset(
        slug: str,
        folder: Path | str,
        revision: int | None = None,
        *,
        cache_lifetime: int | None = None,
    ) -> Path:
        assert slug == "demo"
        assert folder == tmp_path
        assert revision == 4
        assert cache_lifetime is None
        return downloaded_path

    monkeypatch.setattr(api_async, "get_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(api_async, "get_dataset_url_async", fake_get_dataset_url)
    monkeypatch.setattr(api_async, "download_dataset_async", fake_download_dataset)

    assert asyncio.run(
        api_async._resolve_dataset_filepath_async("demo", revision=4)
    ) == (downloaded_path)
