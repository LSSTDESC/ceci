Tutorial
========

First, install ceci by following the instructions on the Installation page.

To run the test example you'll need to use the source code

Running a test pipeline
-----------------------

A mock pipeline, which just reads from and writes to a series of small text files, can be run by with the command:

.. code-block:: bash

    ceci tests/test.yml

Making a new pipeline
---------------------

You can use a cookiecutter template to make new pipeline stages.  You can install cookiecutter with ``pip3 install cookiecutter`` and then run:

.. code-block:: bash

    cookiecutter https://github.com/LSSTDESC/pipeline-package-template

And enter a name for your pipeline collection.

This will create a template for your new pipeline stages.  You design your pipeline stages in python files in this new repo - the example in  ``<repo_name>/<repo_name>stage1.py`` shows a template for this, and you can see the "Stages" section for more details.

Your job as a pipeline builder is to make a file like this for each stage in your pipeline, and fill them in.  You can then 