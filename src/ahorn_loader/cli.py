"""Entry point for the ``ahorn-loader`` command-line application."""

from pathlib import Path
from typing import Annotated

import typer

from .validator import Validator

app = typer.Typer()


@app.command()
def download(
    name: Annotated[str, typer.Argument(help="The name of the dataset to download.")],
) -> None:
    """Download the specified dataset from AHORN.

    Parameters
    ----------
    name : str
        The name of the dataset to download.
    """
    raise NotImplementedError("Download logic is not implemented yet.")


@app.command()
def validate(
    path: Annotated[
        Path, typer.Argument(help="The path to the dataset file to validate.")
    ],
) -> None:
    """Validate whether a given file is a valid AHORN dataset.

    Parameters
    ----------
    path : Path
        The path to the dataset file to validate.
    """
    validator = Validator()
    if not validator.validate(path):
        typer.echo("Validation failed.")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
