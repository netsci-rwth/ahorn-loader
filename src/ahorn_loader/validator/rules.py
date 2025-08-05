"""Module with validation rules for a AHORN dataset."""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path


class BaseRule(ABC):
    """Base class for validation rules."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def validate(self) -> bool:
        """Docstring für validate."""
        self.logger.debug("Start validation.")


class FileNameRule(BaseRule):
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


class NetworkLevelMetadataRule(BaseRule):
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
            "Parsed network-level metadata successfully.", metadata=metadata
        )

        if "_format_version" not in metadata:
            self.logger.error("Network-level metadata must contain '_format_version'.")
            return False

        return True
