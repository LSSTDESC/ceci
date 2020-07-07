.. _sites:

Sites
=====

A site is a machine where a pipeline is to be run.  Ceci currently only supports running a pipeline at a single site, not splitting it up between them.

Three sites are currently supported: ``local``, ``cori-batch``, and ``cori-interactive``.

See also the :ref:`launchers` page for how to configure the manager that runs the pipeline.


Common Options
--------------

All sites have these global options:

.. code-block:: yaml

    site:
        name: local
        mpi_command: mpirun -n   # optional
        image: ""                # optional
        volume ""                # optional


``mpi_command`` sets the name of the command used to launch MPI jobs.  Its default depends the site.

``image`` sets the name of a docker/shifter container in which to run jobs.  It defaults to None, meaning not to use a container.

``volume`` sets an option to pass to docker/shifter to mount a directory inside the container.  It takes the form `/path/on/real/machine:/path/inside/container`


Local
-----

The local site is a general one and represents running in a straightforward local environment.  Jobs are run using the python ``subprocess`` module.

.. code-block:: yaml

    site:
        name: local
        max_threads: 2   # optional

``max_threads`` is optional and controls the maximum number of stages run at the same time.  Its default depends on the launcher used.


Cori Interactive
----------------

The ``cori-interactive`` site is used to run jobs interactively on NERSC compute nodes.  You should first use the ``salloc`` command to get an interactive allocation, and then within that run ``ceci``.

There are no additional options for the ``cori-interactive`` site: the number of parallel stages is given by the number of nodes that you ask for in ``salloc``.



Cori Batch
----------

The site ``cori-batch`` runs on the Cori supercomputer at NERSC, and submits jobs to the SLURM batch system.  In this mode, you should call ceci from the login node and stay logged in while the jobs run.

These options can be used for the ``cori-batch`` site:

.. code-block:: yaml

    launcher:
        name: cori
        cpu_type: haswell   # optional
        queue: debug        # optional
        max_jobs: 2         # optional
        account: m1727      # optional
        walltime: 00:30:00  # optional
        setup:  /global/projecta/projectdirs/lsst/groups/WL/users/zuntz/setup-cori
        # ^^ optional

``cpu_type`` is optional and controls which partition of cori is used for jobs, and should be `haswell` or `KNL`.

``queue`` is optional and controls which SLURM queue jobs are launcher on. It can be ``debug``, ``regular``, or ``premium``. See `the nersc documentation <http://docs.nersc.gov/>`_
 for a description of each.

``max_jobs`` is optional and controls the maximum number of SLURM jobs submitted using sbatch.

``account`` is optional and controls the name of the account to which to charge SLURM jobs.  You need to be a member of the associated project to use an account.

``walltime`` is optional and controls the amount of time allocated to each SLURM job.

``setup`` is optional and selects a script to be run at the start of each SLURM job.
