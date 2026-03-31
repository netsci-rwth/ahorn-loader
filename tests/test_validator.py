"""Tests for dataset validator behavior and error handling."""

from pathlib import Path

import pytest

from ahorn_loader.validator import Validator


def test_validate_accepts_valid_plain_text_dataset(tmp_path: Path) -> None:
    """A well-formed plain text dataset passes validation."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n12 {"weight": 1.0}\n12,34 {"time": "2023-01-01T00:00:00+00:00", "weight": 2.0}'
        "\n",
        encoding="utf-8",
    )

    assert Validator().validate(dataset_path)


def test_validate_rejects_mismatched_file_name(tmp_path: Path) -> None:
    """The dataset file name must match the dataset metadata name."""
    dataset_path = tmp_path / "wrong-name.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        '1 {"weight": 1.0}\n'
        '1,2 {"weight": 2.0}\n',
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_returns_false_for_missing_file(tmp_path: Path) -> None:
    """Missing files fail validation gracefully."""
    missing = tmp_path / "does-not-exist.txt"
    assert not Validator().validate(missing)


def test_validate_returns_false_for_directory_path(tmp_path: Path) -> None:
    """Existing paths that are not regular files should fail preflight validation."""
    dataset_dir = tmp_path / "demo.txt"
    dataset_dir.mkdir()

    assert not Validator().validate(dataset_dir)


def test_validate_rejects_unsupported_file_extension(tmp_path: Path) -> None:
    """Only .txt and .txt.gz dataset files should pass the extension preflight rule."""
    dataset_path = tmp_path / "demo.json"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n1 {"weight": 1.0}\n',
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_returns_false_on_malformed_line(tmp_path: Path) -> None:
    """Malformed entries should return False instead of raising."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        "4 his-line-has-no-json\n",
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_returns_false_on_line_missing_metadata_json(tmp_path: Path) -> None:
    """A data line without a metadata JSON part should fail line-format validation."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n4\n',
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_returns_false_on_invalid_metadata_json(tmp_path: Path) -> None:
    """Invalid metadata in the first line should fail validation."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text("4 not-json\n", encoding="utf-8")

    assert not Validator().validate(dataset_path)


def test_validate_returns_false_for_empty_file(tmp_path: Path) -> None:
    """Empty dataset files should fail when reading the first line."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text("", encoding="utf-8")

    assert not Validator().validate(dataset_path)


def test_validate_returns_false_on_oserror_before_first_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read errors before metadata parsing should return False."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text("placeholder\n", encoding="utf-8")

    def fake_iter_lines(self: Validator, dataset_path: Path):  # type: ignore[no-untyped-def]
        raise OSError("boom")
        yield ""

    monkeypatch.setattr(Validator, "_iter_lines", fake_iter_lines)

    assert not Validator().validate(dataset_path)


def test_validate_returns_false_on_oserror_while_reading_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read errors after metadata parsing should return False."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text("placeholder\n", encoding="utf-8")

    def fake_iter_lines(self: Validator, dataset_path: Path):  # type: ignore[no-untyped-def]
        yield '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        raise OSError("boom")

    monkeypatch.setattr(Validator, "_iter_lines", fake_iter_lines)

    assert not Validator().validate(dataset_path)


def test_validate_rejects_metadata_without_revision(tmp_path: Path) -> None:
    """Metadata without a revision field should not pass validation."""
    dataset_path = tmp_path / "demo-no-revision.txt"
    dataset_path.write_text(
        '{"name": "demo-no-revision", "format-version": "0.1"}\n'
        '1 {"weight": 1.0}\n'
        '1,2 {"time": "2023-01-01T00:00:00+00:00", "weight": 2.0}\n',
        encoding="utf-8",
    )
    assert not Validator().validate(dataset_path)


def test_validate_rejects_metadata_without_format_version(tmp_path: Path) -> None:
    """Metadata without a format-version field should not pass validation."""
    dataset_path = tmp_path / "demo-no-format-version.txt"
    dataset_path.write_text(
        '{"name": "demo-no-format-version", "revision": 1}\n'
        '1 {"weight": 1.0}\n'
        '1,2 {"time": "2023-01-01T00:00:00+00:00", "weight": 2.0}\n',
        encoding="utf-8",
    )
    assert not Validator().validate(dataset_path)


def test_validate_accepts_nodes_before_edges(tmp_path: Path) -> None:
    """Node entries before edge entries are valid."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        '1 {"weight": 1.0}\n'
        '2 {"weight": 2.0}\n'
        '1,2 {"weight": 3.0}\n',
        encoding="utf-8",
    )

    assert Validator().validate(dataset_path)


def test_validate_rejects_nodes_after_edges(tmp_path: Path) -> None:
    """Node entries after an edge entry must be rejected."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        '1,2 {"weight": 3.0}\n'
        '3 {"weight": 1.0}\n',
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_rejects_naive_node_timestamp(tmp_path: Path) -> None:
    """Node timestamps without timezone should fail validation."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        '1 {"time": "2023-01-01T00:00:00"}\n'
        '1,2 {"weight": 1.0}\n',
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_rejects_naive_edge_timestamp(tmp_path: Path) -> None:
    """Edge timestamps without timezone should fail validation."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        '1 {"weight": 1.0}\n'
        '1,2 {"time": "2023-01-01T00:00:00"}\n',
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_accepts_timezone_aware_timestamps(tmp_path: Path) -> None:
    """Timestamps with timezone offsets should pass validation."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        '1 {"time": "2023-01-01T00:00:00+00:00"}\n'
        '1,2 {"time": "2023-01-01T01:00:00+01:00"}\n',
        encoding="utf-8",
    )

    assert Validator().validate(dataset_path)


def test_validate_rejects_patch_level_format_version(tmp_path: Path) -> None:
    """Format versions with patch components must be rejected."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1.0"}\n'
        '1 {"weight": 1.0}\n'
        '1,2 {"weight": 2.0}\n',
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_rejects_non_numeric_format_version(tmp_path: Path) -> None:
    """Format versions must contain only numeric major and minor parts."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "v0.1"}\n'
        '1 {"weight": 1.0}\n'
        '1,2 {"weight": 2.0}\n',
        encoding="utf-8",
    )

    assert not Validator().validate(dataset_path)


def test_validate_accepts_string_dataset_path(tmp_path: Path) -> None:
    """String paths should be accepted and converted before validation."""
    dataset_path = tmp_path / "demo.txt"
    dataset_path.write_text(
        '{"name": "demo", "revision": 1, "format-version": "0.1"}\n'
        '1 {"weight": 1.0}\n'
        '1,2 {"weight": 2.0}\n',
        encoding="utf-8",
    )

    assert Validator().validate(str(dataset_path))
