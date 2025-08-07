"""Module containing the validator for AHORN datasets."""

import gzip
from pathlib import Path

from .rules import BaseRule, FileNameRule, NetworkLevelMetadataRule

__all__ = ["Validator"]


class Validator:
    """Validator class to manage validation rules."""

    def __init__(self) -> None:
        self.rules: list[BaseRule] = [
            FileNameRule(),
            NetworkLevelMetadataRule(),
        ]

    def validate(self, dataset_path: Path | str) -> bool:
        """Run all validation rules.

        Parameters
        ----------
        dataset_path : Path | str
            The path to the dataset file to validate.

        Returns
        -------
        bool
            True if all validation rules pass, False otherwise.
        """
        if isinstance(dataset_path, str):
            dataset_path = Path(dataset_path)

        content: list[str] | None = None

        for rule in self.rules:
            if "file_path" in rule.validate.__code__.co_varnames:
                if not rule.validate(file_path=dataset_path):
                    return False
            elif "content" in rule.validate.__code__.co_varnames:
                # load the content of the file the first time it is needed
                if content is None:
                    if dataset_path.suffix == ".gz":
                        with gzip.open(dataset_path, "rt") as f:
                            content = f.readlines()
                    else:
                        with dataset_path.open() as f:
                            content = f.readlines()
                if not rule.validate(content=content):
                    return False
            else:
                # If the rule does not take any parameters, we call it directly
                if not rule.validate():
                    return False

        return True
