Overview
========

Ceci lets you define and run pipelines - sequences of calculation steps that can depend on earlier steps - and run them under the parsl workflow system (and perhaps in future other systems).

In the ceci model each step in the calculation is defined by writing a python class implementing particular pre-defined methods.

Then you actually run your pipeline by running the ceci command on a configuration file in the YAML format.
  