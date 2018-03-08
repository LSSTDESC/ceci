Pipette
-------

A framework for running DESC pipelines.

This is now alpha status.

You can run an example pipeline from the pipette_lib directory using:

```bash
export PYTHONPATH=$PYTHONPATH:$PWD
export PATH=$PATH:$PWD/bin
pipettte test/test.yaml

```


Roadmap
-------

- MPI on cori and site generation for parsl
- Generating a marker file when a task is completely complete to allow resuming better
- Improved logging
- Data File types
- Metadata operationson FITS and HDF5 files
- Single shared docker/shifter image support
- Export yaml representation of stage inputs etc, for ingestion by javascript GUI
