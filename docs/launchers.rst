.. _launchers:

Launchers
=========

Launchers are the system that actually runs a pipeline, launching and monitoring jobs, checking output, etc.

There are currently two launchers supported by Ceci, ``mini``, and``parsl``, but it's easy for us to add more - please open an issue if you need this.

See also the :ref:`sites` page for how to configure other aspects of where the pipeline is run - different launchers support different site options.

Minirunner
----------

The ``mini`` launcher is a minimal in-built launcher with only basic features, but it's useful for small to medium sized jobs.   

Minirunner understands the concept of Nodes versus Cores on supercomputers, and on NERSC the numbers are determined from SLURM environment variables.   If running on the login node, one node with four cores is assigned.

Minirunner does not launch jobs - if you want to use it in NERSC batch mode you should call it from within the job submission script.

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
