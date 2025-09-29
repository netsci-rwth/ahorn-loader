"""Entry point for the ``ahorn-loader`` command-line application."""

from pathlib import Path
from typing import Annotated

import typer
from rich import print as rich_print
from rich.table import Table

from .api import download_dataset, load_datasets_data
from .validator import Validator

app = typer.Typer()


@app.command()
def ls() -> None:
    """List available datasets in AHORN."""
    try:
        datasets = load_datasets_data(cache_lifetime=3600)
        if "error" in datasets:
            typer.echo(f"Error: {datasets['error']}")
            raise typer.Exit(code=1)
    except Exception as e:
        print(f"Failed to load datasets: {e}")
        raise typer.Exit(code=1) from e

    table = Table(title="Available Datasets")
    table.add_column("Slug", style="cyan")
    table.add_column("Title", style="magenta")
    table.add_column("Tags", style="green")

    for slug, details in datasets.items():
        table.add_row(slug, details["title"], ", ".join(details["tags"]))
    rich_print(table)


@app.command()
def download(
    name: Annotated[str, typer.Argument(help="The name of the dataset to download.")],
    folder: Annotated[
        Path, typer.Argument(help="Folder where the dataset should be saved.")
    ] = Path(),
) -> None:
    """Download the specified dataset from AHORN.

    Parameters
    ----------
    name : str
        The name of the dataset to download.
    folder : Path
        The folder where the dataset should be saved. Defaults to the current directory.
    """
    try:
        download_dataset(name, folder, cache_lifetime=3600)
        typer.echo(f"Downloaded dataset to {folder.absolute()}")
    except Exception as e:
        typer.echo(f"Failed to download dataset: {e}")
        raise typer.Exit(code=1) from e


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
