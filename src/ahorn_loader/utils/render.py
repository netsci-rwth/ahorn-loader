"""Lightweight rendering helpers for terminal-facing output."""

from collections.abc import Sequence


def render_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    """Render a simple plain-text table.

    This is a lightweight alternative to richer table renderers such as
    ``rich.Table`` and avoids an extra dependency.
    """
    if not all(len(row) == len(headers) for row in rows):
        raise ValueError("Number of headers must match number of columns in rows.")

    widths = [
        max(len(header), *(len(row[index]) for row in rows))
        for index, header in enumerate(headers)
    ]

    def format_row(row: Sequence[str]) -> str:
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    separator = "  ".join("-" * width for width in widths)
    table_lines = [
        format_row(headers),
        separator,
        *(format_row(row) for row in rows),
    ]
    return "\n".join(table_lines)
