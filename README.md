# `ahorn-loader`

Library and command-line application to interact with datasets in [AHORN](https://ahorn.rwth-aachen.de/).

## Usage

`ahorn-loader` is both a command-line application and a Python package to interact with the AHORN repository for higher-order datasets.

### Command-Line Usage

To install and use `ahorn-loader` from the command line, you can run the following command:

```bash
uvx ahorn-loader [command] [args]
```

Commands include:
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

# download a dataset
ahorn_loader.download('dataset_name', 'target_path')

# validate a specific dataset (e.g., before adding it to AHORN)
ahorn_loader.validate('path_to_dataset_file')
```
