"""Tests for lightweight rendering helpers."""

import pytest

from ahorn_loader.utils.render import render_table


def test_render_table_formats_a_plain_text_grid() -> None:
    """Tables should be aligned using column widths and simple separators."""
    rendered = render_table(
        ("Slug", "Title"),
        (
            ("a", "Demo"),
            ("longer", "X"),
        ),
    )

    assert rendered == ("Slug    Title\n------  -----\na       Demo \nlonger  X    ")


def test_render_table_rejects_rows_with_wrong_column_count() -> None:
    """Every row must provide exactly one value per header."""
    with pytest.raises(
        ValueError,
        match="Number of headers must match number of columns in rows",
    ):
        render_table(("Slug", "Title"), (("only-one-column",),))
