"""Pytest configuration."""

import pytest

import ahorn_loader


@pytest.fixture(autouse=True)
def doctest_default_imports(
    doctest_namespace: dict[str, object],
) -> None:
    """Add default imports to the doctest namespace.

    This fixture adds the following default imports to every doctest, so that their use
    is consistent across all doctests without boilerplate imports polluting the
    doctests themselves:

    .. code-block:: python

        import ahorn_loader

    Parameters
    ----------
    doctest_namespace : dict
        The namespace of the doctest.
    """
    doctest_namespace["ahorn_loader"] = ahorn_loader
