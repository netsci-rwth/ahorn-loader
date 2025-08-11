"""Module with validation rules for a AHORN dataset."""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path

__all__ = [
    "DatasetRule",
    "FileNameRule",
    "NetworkLevelMetadataRule",
    "PreFlightRule",
]


class PreFlightRule(ABC):
    """Base class for validation rules that run before the dataset file is loaded."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def validate(self, file_path: Path) -> bool:
        """
        Validate the dataset before loading it.

        Parameters
        ----------
        file_path : pathlib.Path
            The path of the file to validate.

        Returns
        -------
        bool
            True if the dataset is valid, False otherwise.
        """


class DatasetRule(ABC):
    """Base class for validation rules that validates the dataset content."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def validate(self, content: list[str]) -> bool:
        """
        Validate the dataset content.

        Parameters
        ----------
        content : list[str]
            The content of the dataset file to validate.

        Returns
        -------
        bool
            True if the dataset content is valid, False otherwise.
        """


class FileNameRule(PreFlightRule):
    """Rule to validate file names."""

    def validate(self, file_path: Path) -> bool:
        """
        Validate the file name against a specific pattern.

        Parameters
        ----------
        file_path : pathlib.Path
            The path of the file to validate.

        Returns
        -------
        bool
            True if the file name is valid, False otherwise.
        """
        if not (file_path.suffix == ".txt" or file_path.name.endswith(".txt.gz")):
            self.logger.error("Dataset must be a .txt or .txt.gz file.")
            return False

        # TODO: Check that the file can be read as plain text or as gzipped text.

        self.logger.debug("File name %s is valid.", file_path.name)
        return True


class NetworkLevelMetadataRule(DatasetRule):
    """Rule to validate network-level metadata."""

    def validate(self, content: list[str]) -> bool:
        """
        Validate the network-level metadata.

        Parameters
        ----------
        content : list[str]
            The content of the dataset file to validate.

        Returns
        -------
        bool
            True if the metadata is valid, False otherwise.
        """
        try:
            metadata = json.loads(content[0])
        except json.JSONDecodeError:
            self.logger.error("First line of the dataset must be valid JSON metadata.")
            return False
        self.logger.debug(
            "Parsed network-level metadata successfully.", extra={"metadata": metadata}
        )

        if "_format_version" not in metadata:
            self.logger.error("Network-level metadata must contain '_format_version'.")
            return False

        return True
