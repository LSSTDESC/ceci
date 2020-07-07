.. _launchers:

Launchers
=========

Launchers are the system that actually runs a pipeline, launching and monitoring jobs, checking output, etc.

There are currently three launchers supported by Ceci, ``mini``, ``parsl``, and ``cwl``, but it's easy for us to add more - please open an issue if you need this.

See also the :ref:`sites` page for how to configure other aspects of where the pipeline is run - different launchers support different site options.

Minirunner
----------

The ``mini`` launcher is a minimal in-built launcher with only basic features, but it's useful for small to medium sized jobs.   

Minirunner understands the concept of Nodes versus Cores on supercomputers, and on Cori the numbers are determined from SLURM environment variables.   If running on the login node, one node with four cores is assigned.

Minirunner does not launch jobs - if you want to use it in Cori batch mode you should call it from within the job submission script.

Minirunner options
^^^^^^^^^^^^^^^^^^

The minirunner has one option, which is common to all sites:

.. code-block:: yaml

    launcher:
        name: mini
        interval: 3      # optional

``interval`` is optional and controls number of seconds between checks that each stage is complete.  It defaults to three seconds.



Parsl
-----

Parsl is a fully-featured workflow manager.  It can be configured for a very wide variety of machines and systems.  It knows how to submit jobs using SLURM and other systems.


Parsl options
^^^^^^^^^^^^^

Parsl has one option, which is common to all sites:

.. code-block:: yaml

    launcher:
        name: parsl
        log: ""       # optional

``log`` chooses a file in which to put overall top-level parsl output, describing the monitoring of jobs and output.


CWL
---

Common Workflow Language is a general language for describing workflows, that can be imported by multiple workflow engines.  A reference implementation called ``cwltool`` can be used locally to run CWL pipelines.

CWL options
^^^^^^^^^^^

CWL has one option, which is common to all sites:

.. code-block:: yaml

    launcher:
        name: cwl
        dir: <path>       # required
        launch: cwltool   # optional

``dir`` controls the directory where the CWL files describing the pipeline and the individual jobs are saved. If it does not exist it will be created.

``launch`` controls the executable run on the CWL files.  The default `cwltool` is actually expanded to ``cwltool --outdir {output_dir} --preserve-environment PYTHONPATH``.
