Pipeline Stages
===============

Overview
--------
A PipelineStage implements a single calculation step within a wider pipeline.
Each different type of analysis stge is represented by a subclass of PipelineStage.  

The base class handles the connection between different pipeline
stages, and the execution of the stages within a workflow system (parsl or cwl),
potentially in parallel (MPI).

The subclasses must:
 - define their name
 - define their inputs and outputs
 - provide a "run" method which does the actual execution of the pipeline step.

They must use base class methods within the run method to find their input
and output file paths.  They can optionally use further methods in this
class to open and prepare those files too.

Inputs/Outputs and Tags
-----------------------
The I/O system for Ceci uses the concept of "tags".
A tag is a string which corresponds to a single input or output file.
Using it allows us to easily connect together pipeline stages by matching
output tags from earlier stages to input tags for later ones.
Tags must be unique across a pipeline.

Pipeline Methods
----------------

The full set of pipeline methods is documented below.
Of particular note are the methods described here, which are designed to be used 
by subclasses.

Return the path to input or output files:

.. code-block:: python

    self.get_input(tag)
    self.get_output(tag)


Get the base class to find and open an input or output file for you,
optionally returning a wrapper class instead of the file:

.. code-block:: python

    self.open_input(tag, wrapper=False, **kwargs)
    self.open_output(tag, wrapper=False, **kwargs)

Look for a section in a yaml input file tagged "config"
and read it.  If the config_options class variable exists in the class
then it checks those options are set or uses any supplied defaults

.. code-block:: python

    self.read_config()

MPI attributes for parallelization. 

.. code-block:: python

    self.rank
    self.size
    self.comm

If the code is not being run in parallel, comm will be None, rank will be 0, 
and size will be 1.



IO tools - reading data in chunks, splitting up according to MPI rank, if used

.. code-block:: python

    self.iterate_fits(tag, hdunum, cols, chunk_rows)
    self.iterate_hdf(tag, group_name, cols, chunk_rows)


Execution
---------
Pipeline stages can be automatically run as part of a pipeline,
or manually run on the command line, using the syntax:

.. code-block:: bash

    python </path/to/pipeline_implementation.py> <StageName> --<input_name1>=</path/to/input1.dat>
        --<input_name2>=</path/to/input2.dat>  --<output_name1>=</path/to/output1.dat>

API
---

The complete pipeline stage API is below - stages not described above 
are mostly used internally by the pipeline system.

 .. autoclass:: ceci.PipelineStage
    :members:
    :member-order: by-source
