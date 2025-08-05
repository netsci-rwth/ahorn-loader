"""Entry point for the ``ahorn-loader`` command-line application."""

import gzip
from pathlib import Path
from typing import Annotated

import typer

from .validator.rules import FileNameRule, NetworkLevelMetadataRule

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
    rules = [
        FileNameRule(),
        NetworkLevelMetadataRule(),
    ]

    content = None
    for rule in rules:
        if "file_path" in rule.validate.__code__.co_varnames:
            rule.validate(file_path=path)
        elif "content" in rule.validate.__code__.co_varnames:
            # load the content of the file the first time it is needed
            if content is None:
                if path.suffix == ".gz":
                    with gzip.open(path, "rt") as f:
                        content = f.readlines()
                else:
                    with path.open() as f:
                        content = f.readlines()
            rule.validate(content=content)
        else:
            # If the rule does not take any parameters, we call it directly
            rule.validate()


if __name__ == "__main__":
    app()
