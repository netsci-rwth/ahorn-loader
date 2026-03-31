"""Pydantic data models to validate AHORN datasets.

This module provides Pydantic models for representing a dataset in AHORN, including
models for individual pieces such as node and edge metadata.
"""

import datetime
import re
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator


def _timezone_aware_datetime_validator(
    value: datetime.datetime,
) -> datetime.datetime:
    """Ensure timestamps include timezone information when provided."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must include timezone information.")

    return value


class NodeMetadata(BaseModel):
    """Metadata for a node in the network.

    Attributes
    ----------
    year : int, optional
        The year associated with the node (if any).
    date : datetime.date, optional
        The date associated with the node (if any).
    time : datetime.datetime, optional
        The timestamp associated with the node (if any).
    weight : float, optional
        The weight of the node (if any).
    """

    year: int | None = Field(ge=1000, lt=9999, default=None)
    date: datetime.date | None = None
    time: Annotated[
        datetime.datetime | None, AfterValidator(_timezone_aware_datetime_validator)
    ] = None
    weight: float | None = None

    model_config = ConfigDict(extra="allow")


class Node(BaseModel):
    """Representation of a node in the network.

    Attributes
    ----------
    id : str
        The unique identifier of the node.
    metadata : NodeMetadata
        The metadata associated with the node.
    """

    id: str
    metadata: NodeMetadata

    model_config = ConfigDict(extra="forbid")


class EdgeMetadata(BaseModel):
    """Metadata for an edge in the network.

    Attributes
    ----------
    year : int, optional
        The year associated with the edge (if any).
    date : datetime.date, optional
        The date associated with the edge (if any).
    time : datetime.datetime, optional
        The timestamp associated with the edge (if any).
    weight : float, optional
        The weight of the edge (if any).
    """

    year: int | None = Field(ge=1000, lt=9999, default=None)
    date: datetime.date | None = None
    time: Annotated[
        datetime.datetime | None, AfterValidator(_timezone_aware_datetime_validator)
    ] = None
    weight: float | None = None

    model_config = ConfigDict(extra="allow")


class Edge(BaseModel):
    """Representation of an edge in the network.

    Edges in AHORN datasets can connect two or more nodes.

    Attributes
    ----------
    elements : list[str]
        List of node identifiers that form the edge.
    metadata : EdgeMetadata
        The metadata associated with the edge.
    """

    elements: list[str] = Field(min_length=2)
    metadata: EdgeMetadata

    model_config = ConfigDict(extra="forbid")


class DatasetMetadata(BaseModel):
    """Metadata for the dataset.

    Attributes
    ----------
    name : str
        The name of the dataset.
    format_version : str
        The version of the AHORN format used, as ``major.minor``.
    """

    name: str
    revision: int
    format_version: str = Field(alias="format-version")

    @field_validator("format_version")
    @classmethod
    def validate_format_version(cls, value: str) -> str:
        """Require format version to be exactly ``numeric.numeric``."""
        if not re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)", value):
            raise ValueError(
                "format-version must have the form 'numeric.numeric' (for example '0.1')."
            )

        return value

    model_config = ConfigDict(extra="allow")


class Dataset(BaseModel):
    """Representation of an AHORN dataset."""

    metadata: DatasetMetadata
    nodes: list[Node] = Field(default_factory=list)
    edges: list[Edge] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")
