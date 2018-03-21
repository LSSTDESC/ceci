Tutorial
========

First, install pipette by following the instructions on the Installation page.

To run the test example you'll need to use the source code

Running a test pipeline
-----------------------

A mock pipeline, which just reads from and writes to a series of small text files, can be run by with the command:

.. code-block:: bash

    pipette test/test.yml

Making a new pipeline
---------------------

To make new pipeline stages, you:

- make a new python package somewhere else, to contain your stages.
- the package must have an __init__.py file that should import from . all the stages you want to use.
- it must also have a file __main__.py with the same contents as the example in pipete_lib.
- each stage is its own class inheriting from pipette.PipelineStage. Each must define its name, inputs, and outputs, and a run method.
- the run method should use the parent methods from PipelineStage to get its inputs and outputs etc.
