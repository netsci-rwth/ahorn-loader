"""Entry point for the ``ahorn-loader`` command-line application."""

from __future__ import annotations

import sys
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, CliApp, CliPositionalArg, CliSubCommand

from .api_async import download_dataset_async, load_datasets_data_async
from .api_sync import validate_dataset
from .utils.render import render_table


class ListCommand(BaseModel):
    """List available datasets in AHORN."""

    async def cli_cmd(self) -> None:
        """Execute the ``ls`` command."""
        try:
            datasets = await load_datasets_data_async(cache_lifetime=3600)
        except Exception as exc:
            print(f"Failed to load datasets: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        rows = [
            (slug, details["title"], ", ".join(details["tags"]))
            for slug, details in datasets.items()
        ]
        print(render_table(("Slug", "Title", "Tags"), rows))


class DownloadCommand(BaseModel):
    """Download a dataset from AHORN."""

    name: CliPositionalArg[str] = Field(
        description="The name of the dataset to download."
    )
    folder: CliPositionalArg[Path] = Field(
        default=Path(),
        description="Folder where the dataset should be saved.",
    )
    revision: int | None = Field(
        default=None,
        description="Revision number to download (defaults to latest).",
    )

    async def cli_cmd(self) -> None:
        """Execute the ``download`` command."""
        try:
            await download_dataset_async(
                self.name,
                self.folder,
                self.revision,
                cache_lifetime=3600,
            )
        except Exception as exc:
            print(f"Failed to download dataset: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        print(f"Downloaded dataset to {self.folder.absolute()}")


class ValidateCommand(BaseModel):
    """Validate whether a given file is a valid AHORN dataset."""

    path: CliPositionalArg[Path] = Field(
        description="The path to the dataset file to validate."
    )

    def cli_cmd(self) -> None:
        """Execute the ``validate`` command."""
        if validate_dataset(self.path):
            print("Validation successful.")
            return

        print("Validation failed.")
        raise SystemExit(1)


class AhornLoaderCli(BaseSettings, cli_prog_name="ahorn-loader"):
    """Command-line interface for the ``ahorn-loader`` package."""

    ls: CliSubCommand[ListCommand]
    download: CliSubCommand[DownloadCommand]
    validate_cmd: CliSubCommand[ValidateCommand] = Field(alias="validate")

    def cli_cmd(self) -> None:
        """Execute the selected subcommand."""
        CliApp.run_subcommand(self)


def app(cli_args: list[str] | None = None) -> None:
    """Console script entrypoint."""
    CliApp.run(AhornLoaderCli, cli_args=cli_args)
