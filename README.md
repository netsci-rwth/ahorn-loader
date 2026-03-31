# `ahorn-loader`

Library and command-line application to interact with datasets in [AHORN](https://ahorn.rwth-aachen.de/).

<div align="center">

[![Python](https://img.shields.io/badge/python-3.12+-blue)](https://www.python.org/)
[![License](https://badgen.net/github/license/netsci-rwth/ahorn-loader)](https://github.com/netsci-rwth/ahorn-loader/blob/main/LICENSE)
[![Codecov](https://codecov.io/gh/netsci-rwth/ahorn-loader/graph/badge.svg)](https://codecov.io/gh/netsci-rwth/ahorn-loader)

</div>

## Usage

`ahorn-loader` is both a command-line application and a Python package to interact with the AHORN repository for higher-order datasets.

### Command-Line Usage

To install and use `ahorn-loader` from the command line, you can run the following command:

```bash
uvx ahorn-loader [command] [args]
```

Commands include:

- `ls`: List available datasets in AHORN.
- `download`: Download a dataset from AHORN.
- `validate`: Validate a specific dataset file (e.g., before adding it to AHORN).

To get a full help of available commands and options, run `ahorn-loader --help`.

### Python Package Usage

To use `ahorn-loader` as a Python package, you can install it via `pip` (or some other package manager of your choice):

```bash
pip install ahorn-loader
```

Then, you can use it in your Python scripts:

```python
import ahorn_loader

# Download the latest revision of a dataset:
ahorn_loader.download_dataset("dataset_name", "target_path")

# Download a specific revision of a dataset:
ahorn_loader.download_dataset("dataset_name", "target_path", revision=3)

# Download and read a dataset:
# The dataset will be stored in your system's cache. For a more permanent storage
# location, use `ahorn_loader.download_dataset` instead.
with ahorn_loader.read_dataset("dataset_name") as dataset:
    for line in dataset:
        ...

# Validate a specific dataset (e.g., before adding it to AHORN):
ahorn_loader.validate_dataset("path_to_dataset_file")
```

`ahorn-loader` also provides an asynchronous API, which you can use for non-blocking contexts.
Asynchronous functions are suffixed with `_async` and available for all operations.

```python
import asyncio
import ahorn_loader


async def main() -> None:
    await ahorn_loader.download_dataset_async("dataset_name", "target_path")

    async with ahorn_loader.read_dataset_async("dataset_name") as dataset:
        for line in dataset:
            ...


asyncio.run(main())
```

## Funding

<img align="right" width="200" src="https://raw.githubusercontent.com/netsci-rwth/ahorn/main/public/images/erc_logo.png">

Funded by the European Union (ERC, HIGH-HOPeS, 101039827).
Views and opinions expressed are however those of the author(s) only and do not necessarily reflect those of the European Union or the European Research Council Executive Agency.
Neither the European Union nor the granting authority can be held responsible for them.
