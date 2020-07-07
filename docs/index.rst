.. ceci documentation master file, created by
   sphinx-quickstart on Wed Mar 14 23:10:34 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Ceci's documentation!
===================================

Ceci is a framework for defining and running DESC pipelines under the Parsl workflow management system.  This means it connects together individual tasks that depend on each other's outputs and runs them, potentially in parallel, passing the outputs of one onto the next.

.. toctree::
   :maxdepth: 1
   :caption: Contents:
   
   installation
   overview
   tutorial
   stages
   config
   launchers
   sites



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
