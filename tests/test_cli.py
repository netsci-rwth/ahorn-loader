"""Tests the command-line interface of the ``ahorn-loader`` package."""

import json
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pytest

import ahorn_loader.cli as cli
from ahorn_loader.cli import app


@dataclass
class CliResult:
    """Captured result of a CLI invocation."""

    exit_code: int
    output: str


def invoke_cli(*args: str) -> CliResult:
    """Run the CLI and capture its text output."""
    stdout = StringIO()
    stderr = StringIO()

    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            app(list(args))
    except SystemExit as exc:
        output = stdout.getvalue() + stderr.getvalue()
        if exc.code is None:
            exit_code = 0
        elif isinstance(exc.code, int):
            exit_code = exc.code
        else:
            output += exc.code
            exit_code = 1

        return CliResult(exit_code=exit_code, output=output)

    return CliResult(exit_code=0, output=stdout.getvalue() + stderr.getvalue())


def test_ls_command() -> None:
    """Tests the 'ls' command returns a plausible list of datasets."""
    result = invoke_cli("ls")
    assert result.exit_code == 0, result.output

    # Check for table header presence
    assert "Slug" in result.output
    assert "Title" in result.output
    assert "Tags" in result.output

    # Check that karate-club dataset appears in the list
    assert "karate-club" in result.output.lower()

    # Verify output contains multiple lines (header + datasets)
    lines = result.output.strip().split("\n")
    assert len(lines) > 5  # At least header + some datasets


def test_ls_command_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """The 'ls' command should report loader failures clearly."""

    async def fake_load_datasets_data_async(
        *, cache_lifetime: int | None = None
    ) -> dict[str, object]:
        assert cache_lifetime == 3600
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "load_datasets_data_async", fake_load_datasets_data_async)

    result = invoke_cli("ls")

    assert result.exit_code == 1
    assert "Failed to load datasets: boom" in result.output


def test_download_command_failure() -> None:
    """Tests the 'download' command failure path of the CLI."""
    result = invoke_cli("download", "test_dataset")
    assert result.exit_code == 1
    assert "Failed to download dataset:" in result.output


def test_download_command_karate_club(tmp_path: Path) -> None:
    """Downloads the karate-club dataset via live API and verifies it is stored and valid."""
    result = invoke_cli("download", "karate-club", str(tmp_path))
    assert result.exit_code == 0, result.output
    assert "Downloaded dataset to" in result.output

    downloaded_file = tmp_path / "karate-club.txt"
    assert downloaded_file.exists()
    assert downloaded_file.is_file()

    with downloaded_file.open() as f:
        first_line = f.readline().strip()
        assert json.loads(first_line) == {
            "name": "karate-club",
            "format-version": "0.3",
            "revision": 2,
        }


def test_validate_command_success(tmp_path: Path) -> None:
    """The validate command should succeed for a valid dataset file."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        '1 {"weight": 1.0}\n'
        '1,2 {"weight": 2.0}\n',
        encoding="utf-8",
    )

    result = invoke_cli("validate", str(dataset_path))

    assert result.exit_code == 0, result.output
    assert "Validation successful." in result.output


def test_validate_command_failure(tmp_path: Path) -> None:
    """The validate command should fail for an invalid dataset file."""
    dataset_path = tmp_path / "invalid.txt"
    dataset_path.write_text(
        '{"name": "demo", "format-version": "0.1"}\n1 {"weight": 1.0}\n',
        encoding="utf-8",
    )

    result = invoke_cli("validate", str(dataset_path))

    assert result.exit_code == 1
    assert "Validation failed." in result.output
