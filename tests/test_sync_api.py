"""Tests for the synchronous AHORN API surface."""

import asyncio
from pathlib import Path

import pytest

import ahorn_loader.api_sync as api_sync


def test_load_datasets_data_sync_runs_async_impl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sync wrapper should execute the async implementation."""
    expected = {
        "demo": {
            "slug": "demo",
            "title": "Demo Dataset",
            "tags": ["toy"],
            "attachments": {},
        }
    }

    async def fake_load_datasets_data(
        *, cache_lifetime: int | None = None
    ) -> dict[str, api_sync.DatasetDict]:
        assert cache_lifetime == 60
        return expected

    monkeypatch.setattr(api_sync, "load_datasets_data_async", fake_load_datasets_data)

    assert api_sync.load_datasets_data(cache_lifetime=60) == expected


def test_read_dataset_sync_reads_resolved_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sync reader should yield an iterator over the cached dataset file."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text('{"name": "demo"}\n1 {"weight": 1.0}\n', encoding="utf-8")

    async def fake_resolve_dataset_filepath(
        slug: str, revision: int | None = None
    ) -> Path:
        assert slug == "demo"
        assert revision == 2
        return dataset_path

    monkeypatch.setattr(
        api_sync,
        "_resolve_dataset_filepath_async",
        fake_resolve_dataset_filepath,
    )

    with api_sync.read_dataset("demo", revision=2) as dataset:
        assert next(dataset).strip() == '{"name": "demo"}'


def test_sync_api_raises_inside_running_event_loop() -> None:
    """The sync wrappers should fail fast when called from async code."""

    async def call_sync_api() -> None:
        with pytest.raises(RuntimeError, match="Use the async API"):
            api_sync.load_datasets_data()

    asyncio.run(call_sync_api())
