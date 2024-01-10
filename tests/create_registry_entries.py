"""
This script creates some test database entries to emulate the DESC data
registry.

Uses the `datareg.Registrar.register_dataset` function to ingest the `dm.txt`
and `fiducial_cosmology.txt` files into the registry.

More details of the data registry at https://github.com/LSSTDESC/dataregistry.
"""

import os
import sys

from dataregistry import DataRegistry

_TEST_ROOT_DIR = "DataRegistry_data"

# Make root dir
if not os.path.isdir(_TEST_ROOT_DIR):
    os.makedirs(_TEST_ROOT_DIR)

# Establish connection to database
datareg = DataRegistry(root_dir=_TEST_ROOT_DIR)

# Add new entry.
datareg.Registrar.register_dataset(
    "dm.txt",
    "0.0.1",
    verbose=True,
    old_location="inputs/dm.txt"
)

datareg.Registrar.register_dataset(
    "fiducial_cosmology.txt",
    "0.0.1",
    verbose=True,
    old_location="inputs/fiducial_cosmology.txt"
)
