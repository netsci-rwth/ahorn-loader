# `ahorn-loader`

Library and command-line application to interact with datasets in [AHORN](https://ahorn.rwth-aachen.de/).

<div align="center">

[![Python](https://img.shields.io/badge/python-3.11+-blue)](https://www.python.org/)
[![license](https://badgen.net/github/license/netsci-rwth/ahorn-loader)](https://github.com/pyt-team/TopoNetX/blob/main/LICENSE)

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

# Download a dataset:
ahorn_loader.download_dataset("dataset_name", "target_path")

# Download and read a dataset:
# The dataset will be stored in your system's cache. For a more permanent storage
# location, use `ahorn_loader.download_dataset` instead.
with ahorn_loader.read_dataset("dataset_name") as dataset:
    for line in dataset:
        ...

# Validate a specific dataset (e.g., before adding it to AHORN):
ahorn_loader.validate("path_to_dataset_file")
```

## Funding

<img align="right" width="200" src="https://raw.githubusercontent.com/netsci-rwth/ahorn/main/public/images/erc_logo.png">

Funded by the European Union (ERC, HIGH-HOPeS, 101039827).
Views and opinions expressed are however those of the author(s) only and do not necessarily reflect those of the European Union or the European Research Council Executive Agency.
Neither the European Union nor the granting authority can be held responsible for them.
