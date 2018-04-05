Ceci Pipeline Software
----------------------
[![Build Status](https://travis-ci.org/LSSTDESC/ceci.svg?branch=master)](https://travis-ci.org/LSSTDESC/ceci)

A lightweight parsl-based framework for running DESC pipelines. Documentation available here: http://ceci.readthedocs.io

This is now alpha status.

## Install

```bash
pip install git+git://github.com/EiffL/python-cwlgen.git
python setup.py install
```

You can then run an example pipeline from the ceci_lib directory using:

```bash
export PYTHONPATH=$PYTHONPATH:$PWD
ceci test/test.yml
```

## Roadmap

- MPI on cori and site generation for parsl
- Generating a marker file when a task is completely complete to allow resuming better
- Improved logging
- Data File types
- Metadata operationson FITS and HDF5 files
- Single shared docker/shifter image support
- Export yaml representation of stage inputs etc, for ingestion by javascript GUI


Adding Pipeline Stages
----------------------

To make new pipeline stages, you must:

- make a new python package somewhere else, to contain your stages.
- the package must have an `__init__.py` file that should import from `.` all the stages you want to use.
- it must also have a file `__main__.py` with the same contents as the example in `pipete_lib`.
- each stage is its own class inheriting from `ceci.PipelineStage`. Each must define its name, inputs, and outputs, and a run method.
- the run method should use the parent methods from `PipelineStage` to get its inputs and outputs etc.
