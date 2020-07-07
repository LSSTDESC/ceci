.. _config2:

Config YAML files
=================

The second configuration file needed for a pipeline configures individual stages it is running. 

Each pipeline stage specified any configuration options it can take as part of its class definition, in the ``config_options`` dictionary.   This can either specify a default value for the config option, or if there is no sensible default, the type of the option expected (str, int, etc.).


Search sequence
---------------

The following places will be searched for config values:

- The command line (if you are running the stage stand-alone, not as part of a pipeline)
- The stage's section in this file
- The ``global`` section in this file
- Any default value specified in the ``config_options``

If no value is found and there is no default, and error is raised.


Here's an example file:

.. code-block:: yaml

    global:
        chunk_rows: 100000
        pixelization: healpix
        nside: 512
        sparse: True

    TXGCRTwoCatalogInput:
        metacal_dir: /global/cscratch1/sd/desc/DC2/data/Run2.2i/dpdd/Run2.2i-t3828/metacal_table_summary
        photo_dir: /global/cscratch1/sd/desc/DC2/data/Run2.2i/dpdd/Run2.2i-t3828/object_table_summary

    TXIngestRedmagic:
        lens_zbin_edges: [0.1, 0.3, 0.5]

    PZPDFMLZ:
        nz: 301
        zmax: 3.0

    ...

The keys here are the names of pipeline stages.  The ``global`` section can be searched by any stage.
