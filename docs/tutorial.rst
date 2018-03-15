Tutorial
========

Extend the pipeline
-------------------

To make new pipeline stages, you must:

  - make a new python package somewhere else, to contain your stages.
  - the package must have an __init__.py file that should import from . all the stages you want to use.
  - it must also have a file __main__.py with the same contents as the example in pipete_lib.
  - each stage is its own class inheriting from pipette.PipelineStage. Each must define its name, inputs, and outputs, and a run method.
  - the run method should use the parent methods from PipelineStage to get its inputs and outputs etc.
