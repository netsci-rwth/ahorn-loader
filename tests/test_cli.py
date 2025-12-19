"""Tests the command-line interface of the ``ahorn-loader`` package."""

from pathlib import Path

from typer.testing import CliRunner

from ahorn_loader.cli import app

runner = CliRunner()


def test_ls_command() -> None:
    """Tests the 'ls' command returns a plausible list of datasets."""
    result = runner.invoke(app, ["ls"])
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


def test_download_command_failure() -> None:
    """Tests the 'download' command failure path of the CLI."""
    result = runner.invoke(app, ["download", "test_dataset"])
    assert result.exit_code == 1
    assert "Failed to download dataset:" in result.output


def test_download_command_karate_club(tmp_path: Path) -> None:
    """Downloads the karate-club dataset via live API and verifies it is stored and valid."""
    result = runner.invoke(app, ["download", "karate-club", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "Downloaded dataset to" in result.output

    downloaded_file = tmp_path / "karate-club.txt"
    assert downloaded_file.exists()
    assert downloaded_file.is_file()

    with downloaded_file.open() as f:
        first_line = f.readline().strip()
        assert first_line == '{"name": "karate-club", "_format-version": "0.1"}'
