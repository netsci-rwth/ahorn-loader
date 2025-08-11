"""Module containing the validator for AHORN datasets."""

import gzip
from pathlib import Path

from .rules import DatasetRule, FileNameRule, NetworkLevelMetadataRule, PreFlightRule

__all__ = ["Validator"]


class Validator:
    """Validator class to manage validation rules."""

    pre_flight_rules: list[PreFlightRule]
    dataset_rules: list[DatasetRule]

    def __init__(self) -> None:
        self.pre_flight_rules = [
            FileNameRule(),
        ]

        self.dataset_rules = [
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

        if not all(
            rule.validate(file_path=dataset_path) for rule in self.pre_flight_rules
        ):
            return False

        if dataset_path.suffix == ".gz":
            with gzip.open(dataset_path, "rt") as f:
                content = f.readlines()
        else:
            with dataset_path.open() as f:
                content = f.readlines()

        return all(rule.validate(content=content) for rule in self.dataset_rules)
