"""Module containing the validator for AHORN datasets."""

import gzip
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from .model import Dataset, DatasetMetadata, Edge, EdgeMetadata, Node, NodeMetadata

__all__ = [
    "FileExistsRule",
    "FileExtensionRule",
    "FileNameRule",
    "PostModelRule",
    "PreFlightRule",
    "Validator",
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


class PostModelRule(ABC):
    """Base class for validation rules that run after the dataset file is loaded."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def validate(self, file_path: Path, dataset: Dataset) -> bool:
        """
        Validate the dataset after it was loaded into a pydantic model.

        Parameters
        ----------
        file_path : pathlib.Path
            The path of the file to validate.
        dataset : Dataset
            The dataset to validate.

        Returns
        -------
        bool
            True if the dataset is valid, False otherwise.
        """


class FileExtensionRule(PreFlightRule):
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

        self.logger.debug("File name %s is valid.", file_path.name)
        return True


class FileExistsRule(PreFlightRule):
    """Rule to validate that the input path points to an existing file."""

    def validate(self, file_path: Path) -> bool:
        """Validate that the file exists and is a regular file."""
        if not file_path.exists():
            self.logger.error("Dataset file %s does not exist.", file_path)
            return False

        if not file_path.is_file():
            self.logger.error("Path %s is not a file.", file_path)
            return False

        self.logger.debug("Dataset file %s exists.", file_path)
        return True


class FileNameRule(PostModelRule):
    """Rule to validate file names based on dataset metadata."""

    def validate(self, file_path: Path, dataset: Dataset) -> bool:
        """
        Validate the file name against the dataset metadata.

        Parameters
        ----------
        file_path : pathlib.Path
            The path of the file to validate.
        dataset : Dataset
            The dataset to validate.

        Returns
        -------
        bool
            True if the file name is valid, False otherwise.
        """
        if not (
            file_path.name == f"{dataset.metadata.name}.txt"
            or file_path.name == f"{dataset.metadata.name}.txt.gz"
        ):
            self.logger.error(
                "File name %s does not match expected name %s or %s.",
                file_path.name,
                f"{dataset.metadata.name}.txt",
                f"{dataset.metadata.name}.txt.gz",
            )
            return False

        self.logger.debug("File name %s matches expected name.", file_path.name)
        return True


class Validator:
    """Validator class to manage validation rules."""

    pre_flight_rules: list[PreFlightRule]
    post_model_rules: list[PostModelRule]

    def __init__(self) -> None:
        self.pre_flight_rules = [
            FileExistsRule(),
            FileExtensionRule(),
        ]
        self.post_model_rules = [
            FileNameRule(),
        ]

        self.logger = logging.getLogger(self.__class__.__name__)

    def _iter_lines(self, dataset_path: Path) -> Iterator[str]:
        """Yield lines from plain text or gzip-compressed dataset files."""
        if dataset_path.suffix == ".gz":
            with gzip.open(dataset_path, "rt", encoding="utf-8") as f:
                yield from f
        else:
            with dataset_path.open("r", encoding="utf-8") as f:
                yield from f

    def _validate_preflight(self, dataset_path: Path) -> bool:
        """Run all pre-flight validation rules."""
        return all(
            rule.validate(file_path=dataset_path) for rule in self.pre_flight_rules
        )

    def _validate_post_model(self, dataset_path: Path, dataset: Dataset) -> bool:
        """Run all post-model validation rules."""
        return all(
            rule.validate(file_path=dataset_path, dataset=dataset)
            for rule in self.post_model_rules
        )

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

        if not self._validate_preflight(dataset_path):
            self.logger.error("Pre-flight validation failed for %s.", dataset_path)
            return False

        try:
            line_iterator = self._iter_lines(dataset_path)
            first_line = next(line_iterator).strip()
        except StopIteration:
            self.logger.error("Dataset file %s is empty.", dataset_path)
            return False
        except OSError as error:
            self.logger.error("Could not read dataset file %s: %s", dataset_path, error)
            return False

        try:
            dataset_metadata = DatasetMetadata.model_validate_json(first_line)
        except ValidationError as error:
            self.logger.error("Invalid dataset metadata in first line: %s", error)
            return False

        nodes: list[Node] = []
        edges: list[Edge] = []
        has_seen_edge = False

        try:
            for line_number, line in enumerate(line_iterator, start=2):
                parts = line.split(maxsplit=1)
                if len(parts) != 2:
                    self.logger.error(
                        "Invalid line format at line %d. Expected '<elements> <json-metadata>'.",
                        line_number,
                    )
                    return False

                elements_raw, metadata_raw = parts
                if "," in elements_raw:
                    has_seen_edge = True
                    edge_metadata = EdgeMetadata.model_validate_json(metadata_raw)
                    edge_elements = [
                        element.strip() for element in elements_raw.split(",")
                    ]
                    edges.append(Edge(elements=edge_elements, metadata=edge_metadata))
                else:
                    if has_seen_edge:
                        self.logger.error(
                            "Invalid node/edge ordering at line %d. "
                            "Node entries must appear before any edge entries.",
                            line_number,
                        )
                        return False
                    node_metadata = NodeMetadata.model_validate_json(metadata_raw)
                    nodes.append(Node(id=elements_raw, metadata=node_metadata))
        except ValidationError as error:
            self.logger.error("Invalid node or edge entry: %s", error)
            return False
        except OSError as error:
            self.logger.error("Could not read dataset file %s: %s", dataset_path, error)
            return False

        dataset = Dataset(metadata=dataset_metadata, nodes=nodes, edges=edges)

        self.logger.debug(
            "Pydantic data models constructed successfully.",
            extra={
                "dataset_metadata": dataset_metadata,
                "nodes": nodes,
                "edges": edges,
            },
        )

        if not self._validate_post_model(dataset_path, dataset):
            self.logger.error("Post-model validation failed for %s.", dataset_path)
            return False

        return True
